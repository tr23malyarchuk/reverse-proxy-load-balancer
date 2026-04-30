#!/bin/bash
# scriptB.sh  –  synthetic load generator for testing
#
# Sends /sort?size=N requests to whichever containers are currently running.
# By default uses ALL running managed containers (round-robin across them)
# so that main.py's balancing logic can be observed.
#
# Usage:
#   bash scriptB.sh               # default: size=1000000, interval 5-10 s
#   bash scriptB.sh 500000 2 8    # size=500000, interval 2-8 s

set -euo pipefail

SORT_SIZE="${1:-1000000}"
MIN_SLEEP="${2:-5}"
MAX_SLEEP="${3:-10}"

declare -A PORT=( [srv1]=8081 [srv2]=8082 [srv3]=8083 [srv4]=8084 )
CONTAINER_ORDER=( srv1 srv2 srv3 srv4 )

container_running() {
    docker ps --format '{{.Names}}' | grep -q "^$1\$"
}

get_running_containers() {
    local running=()
    for name in "${CONTAINER_ORDER[@]}"; do
        container_running "$name" && running+=("$name")
    done
    echo "${running[@]:-}"
}

rr_index=0

next_container_rr() {
    local -a running=("$@")
    local count=${#running[@]}
    (( count == 0 )) && { echo ""; return; }
    echo "${running[$(( rr_index % count ))]}"
    rr_index=$(( rr_index + 1 ))
}

echo "[scriptB] Load generator started. sort_size=$SORT_SIZE, interval=${MIN_SLEEP}-${MAX_SLEEP}s"
echo "[scriptB] Press Ctrl+C to stop."
echo ""

while true; do
    read -ra running <<< "$(get_running_containers)"

    if (( ${#running[@]} == 0 )); then
        echo "[scriptB] No running containers – waiting..."
        sleep 5
        continue
    fi

    target=$(next_container_rr "${running[@]}")
    port=${PORT[$target]}

    echo "[scriptB] → $target (port $port)  /sort?size=$SORT_SIZE"
    curl -s --max-time 120 \
        "http://localhost:${port}/sort?size=${SORT_SIZE}" > /dev/null &

    sleep $(( RANDOM % (MAX_SLEEP - MIN_SLEEP + 1) + MIN_SLEEP ))
done