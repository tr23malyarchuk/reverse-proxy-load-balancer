#!/bin/bash
# scriptA.sh – manages container lifecycle

set -euo pipefail

IMAGE_NAME="tr23malyarchuk/pa-tr23malyarchuk:latest"
NETWORK_NAME="lb-net"
MAX_BUSY_COUNT=2     # minutes before scaling
MAX_IDLE_COUNT=6     # minutes of waiting before stop

# Container metadata
declare -A PORT=( [srv1]=8081 [srv2]=8082 [srv3]=8083 [srv4]=8084 )

container_running() {
    docker ps --format '{{.Names}}' | grep -q "^$1\$"
}

container_exists() {
    docker ps -a --format '{{.Names}}' | grep -q "^$1\$"
}

remove_container() {
    local name=$1
    if container_exists "$name"; then
        echo "[scriptA] Removing container $name..."
        docker rm -f "$name" > /dev/null 2>&1
    fi
}

launch_container() {
    local name=$1
    local port=${PORT[$name]}
    echo "[scriptA] Starting $name (host port $port)..."
    
    docker network create "$NETWORK_NAME" 2>/dev/null || true
    
    docker run -d \
        --name "$name" \
        --network "$NETWORK_NAME" \
        --network-alias "$name" \
        -p "$port:8000" \
        "$IMAGE_NAME" > /dev/null
    
    echo "[scriptA] $name is up at http://$name:8000 (host port $port)"
}

get_cpu_usage() {
    local name=$1
    docker stats --no-stream --format "{{.CPUPerc}}" "$name" 2>/dev/null \
        | sed 's/%//' | tr -d ' '
}

monitor_until_scaleout_or_idle() {
    local container=$1
    local next=$2
    local busy_count=0
    local idle_count=0

    echo "[scriptA] Monitoring $container..."

    while true; do
        if ! container_running "$container"; then
            echo "[scriptA] $container disappeared. Exiting."
            exit 1
        fi

        local cpu
        cpu=$(get_cpu_usage "$container")
        
        if [[ -z "$cpu" ]]; then
            echo "[scriptA] Could not read CPU for $container – skipping."
            sleep 60
            continue
        fi

        echo "[scriptA] $container CPU=${cpu}% (busy=${busy_count} idle=${idle_count})"

        if (( $(echo "$cpu > 30.0" | bc -l) )); then
            busy_count=$(( busy_count + 1 ))
            idle_count=0
        else
            idle_count=$(( idle_count + 1 ))
            busy_count=0
        fi

        if (( idle_count >= MAX_IDLE_COUNT )); then
            echo "[scriptA] System idle. Shutting down."
            shutdown_all
            exit 0
        fi

        if (( busy_count >= MAX_BUSY_COUNT )); then
            echo "[scriptA] $container busy for $MAX_BUSY_COUNT min."
            if [[ -n "$next" ]]; then
                remove_container "$next"
                launch_container "$next"
                echo "[scriptA] Scaled out to $next."
            else
                echo "[scriptA] At max capacity (srv4)."
            fi
            return
        fi

        sleep 60
    done
}

shutdown_all() {
    echo "[scriptA] Stopping all managed containers..."
    for name in "${!PORT[@]}"; do
        if container_running "$name"; then
            docker stop "$name" > /dev/null 2>&1 || true
            docker rm -f "$name" > /dev/null 2>&1 || true
        fi
    done
    echo "[scriptA] All containers stopped."
}

cleanup_on_interrupt() {
    echo ""
    echo "[scriptA] Caught SIGINT – cleaning up."
    shutdown_all
    exit 0
}

trap cleanup_on_interrupt SIGINT SIGTERM

# Main
echo "[scriptA] Starting up."
remove_container "srv1"
launch_container "srv1"
echo "[scriptA] srv1 running. Press Ctrl+C to stop."

while true; do
    monitor_until_scaleout_or_idle "srv1" "srv2"
    if container_running "srv2"; then
        monitor_until_scaleout_or_idle "srv2" "srv3"
    fi
    if container_running "srv3"; then
        monitor_until_scaleout_or_idle "srv3" "srv4"
    fi
    if container_running "srv4"; then
        monitor_until_scaleout_or_idle "srv4" ""
    fi
    sleep 10
done