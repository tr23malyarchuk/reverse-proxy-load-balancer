#!/bin/bash
# scriptA.sh  –  manages container lifecycle
#
# Starts srv1 on CPU core 0.  Monitors CPU load each minute.
# If busy for MAX_BUSY_COUNT consecutive minutes  → scales out (starts next container).
# If idle for MAX_IDLE_COUNT consecutive minutes  → shuts everything down.
# Polls Docker Hub every 2 min for a new image; does a rolling update if found.
#
# Usage: bash scriptA.sh

set -euo pipefail

IMAGE_NAME="tr23malyarchuk/pa-tr23malyarchuk:latest"
NETWORK_NAME="lb-net"
MAX_BUSY_COUNT=2     # minutes busy before scale-out
MAX_IDLE_COUNT=10     # minutes idle before shutdown

# Container metadata (name → cpu_core, name → port)
declare -A CPU_CORE=( [srv1]=0 [srv2]=1 [srv3]=2 [srv4]=3 )
declare -A PORT=(     [srv1]=8081 [srv2]=8082 [srv3]=8083 [srv4]=8084 )
CONTAINER_ORDER=( srv1 srv2 srv3 srv4 )

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

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
    local core=${CPU_CORE[$name]}
    local port=${PORT[$name]}
    echo "[scriptA] Starting $name  (CPU core $core, port $port)..."
    # Ensure network exists
    docker network create "$NETWORK_NAME" 2>/dev/null || true
    docker run -d \
        --name "$name" \
        --cpuset-cpus="$core" \
        --network "$NETWORK_NAME" \
        -p "$port:8081" \
        "$IMAGE_NAME" > /dev/null
    echo "[scriptA] $name is up."
}

# ---------------------------------------------------------------------------
# CPU monitoring
# ---------------------------------------------------------------------------

# Returns the CPU% of a running container as a plain number (e.g. "12.5")
get_cpu_usage() {
    local name=$1
    # docker stats outputs e.g. "12.50%"
    docker stats --no-stream --format "{{.CPUPerc}}" "$name" 2>/dev/null \
        | sed 's/%//' \
        | tr -d ' '
}

# ---------------------------------------------------------------------------
# Scale-out logic
# ---------------------------------------------------------------------------

# Watch $container; when load is high enough → start $next (if set).
# When idle long enough → exit 0 (triggers full shutdown in the outer loop).
monitor_until_scaleout_or_idle() {
    local container=$1
    local next=$2        # may be empty if this is the last container
    local busy_count=0
    local idle_count=0

    echo "[scriptA] Monitoring $container..."

    while true; do
        if ! container_running "$container"; then
            echo "[scriptA] $container disappeared unexpectedly. Exiting."
            exit 1
        fi

        local cpu
        cpu=$(get_cpu_usage "$container")

        # Guard against empty / non-numeric output
        if [[ -z "$cpu" ]]; then
            echo "[scriptA] Could not read CPU for $container – skipping tick."
            sleep 60
            continue
        fi

        echo "[scriptA] $container  CPU=${cpu}%  (busy=${busy_count} idle=${idle_count})"

        if (( $(echo "$cpu > 0.0" | bc -l) )); then
            busy_count=$(( busy_count + 1 ))
            idle_count=0
        else
            idle_count=$(( idle_count + 1 ))
            busy_count=0
        fi

        if (( idle_count >= MAX_IDLE_COUNT )); then
            echo "[scriptA] $container idle for ${MAX_IDLE_COUNT} min. Shutting everything down."
            shutdown_all
            exit 0
        fi

        if (( busy_count >= MAX_BUSY_COUNT )); then
            echo "[scriptA] $container busy for ${MAX_BUSY_COUNT} min."
            if [[ -n "$next" ]]; then
                remove_container "$next"
                launch_container "$next"
                echo "[scriptA] Scaled out to $next."
            else
                echo "[scriptA] Already at maximum capacity (srv4). No more scale-out."
            fi
            return  # caller moves on to monitoring the next container
        fi

        sleep 60
    done
}

# ---------------------------------------------------------------------------
# Shutdown / cleanup
# ---------------------------------------------------------------------------

shutdown_all() {
    echo "[scriptA] Stopping all managed containers..."
    for name in "${CONTAINER_ORDER[@]}"; do
        if container_running "$name"; then
            echo "[scriptA] Stopping $name..."
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

# ---------------------------------------------------------------------------
# Rolling update
# ---------------------------------------------------------------------------

check_for_image_update() {
    echo "[scriptA] Checking for a new image version..."
    if docker pull "$IMAGE_NAME" 2>&1 | grep -q "Downloaded newer image"; then
        echo "[scriptA] New image found – starting rolling update."
        rolling_update
        return 0
    else
        echo "[scriptA] Image is up-to-date."
        return 1
    fi
}

rolling_update() {
    # Keep at least one container alive at all times.
    # Strategy: find all running managed containers; update them one by one
    # except the first one (keep it alive), then update the first last.

    local running=()
    for name in "${CONTAINER_ORDER[@]}"; do
        container_running "$name" && running+=("$name")
    done

    if (( ${#running[@]} == 0 )); then
        echo "[scriptA] No running containers to update."
        return
    fi

    local anchor="${running[0]}"
    echo "[scriptA] Keeping $anchor alive during update."

    for name in "${running[@]}"; do
        [[ "$name" == "$anchor" ]] && continue
        echo "[scriptA] Rolling-updating $name..."
        remove_container "$name"
        launch_container "$name"
        sleep 5   # give it a moment before updating the next one
    done

    # Finally update the anchor
    echo "[scriptA] Updating anchor container $anchor..."
    remove_container "$anchor"
    launch_container "$anchor"
    echo "[scriptA] Rolling update complete."
}

# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

echo "[scriptA] Starting up."
remove_container "srv1"
launch_container "srv1"
echo "[scriptA] srv1 running on port 8081. Press Ctrl+C to stop."
echo ""

while true; do
    check_for_image_update || true

    # Monitor containers in chain: srv1 → srv2 → srv3 → srv4
    # Each call returns when scale-out happens or the whole system goes idle.
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

    sleep 120
done

