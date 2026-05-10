#!/bin/bash
# scriptB.sh  –  load generator for demonstrating scaling
#
# Sends real conversion requests through the load balancer (main.py on port 8000).
# This way main.py distributes load across srv1..srv4,
# and scriptA sees CPU load and scales out.
#
# Usage:
#   bash scriptB.sh               # default settings
#   bash scriptB.sh 1 3           # interval 1-3 seconds (faster)

set -euo pipefail

MIN_SLEEP="${1:-1}"
MAX_SLEEP="${2:-3}"

BALANCER="http://localhost:8000"
DATA_DIR="$(dirname "$0")/data/input"

WAV="${DATA_DIR}/sample.wav"
PDF="${DATA_DIR}/sample.pdf"
WEBP="${DATA_DIR}/sample.webp"
RAR="${DATA_DIR}/sample.rar"

ALGORITHMS=( round_robin random least_connections ip_hash power_of_two )
ENDPOINTS=( "/file-request" "/pdf2png" "/webp2png" "/ziprar" )
FILES=( "$WAV" "$PDF" "$WEBP" "$RAR" )

req_count=0

pick_algo() {
    echo "${ALGORITHMS[$(( RANDOM % ${#ALGORITHMS[@]} ))]}"
}

echo "[scriptB] Load generator started."
echo "[scriptB] Sending requests to balancer at ${BALANCER}"
echo "[scriptB] Interval: ${MIN_SLEEP}-${MAX_SLEEP}s. Press Ctrl+C to stop."
echo ""

while true; do
    # Pick random endpoint + file pair
    idx=$(( RANDOM % ${#ENDPOINTS[@]} ))
    endpoint="${ENDPOINTS[$idx]}"
    file="${FILES[$idx]}"
    algo=$(pick_algo)

    if [[ ! -f "$file" ]]; then
        echo "[scriptB] File not found: $file – skipping"
        sleep 2
        continue
    fi

    req_count=$(( req_count + 1 ))
    echo "[scriptB] #${req_count}  ${endpoint}  algo=${algo}"

    curl -s --max-time 60 \
        -X POST "${BALANCER}${endpoint}" \
        -F "file=@${file}" \
        -F "algorithm=${algo}" \
        -o /dev/null \
        -w "[scriptB] → HTTP %{http_code}  time=%{time_total}s\n" &

    sleep $(( RANDOM % (MAX_SLEEP - MIN_SLEEP + 1) + MIN_SLEEP ))
done
