#!/bin/bash
# start.sh  -  запускає всю систему однією командою
#
# Використання:
#   bash start.sh          # запуск
#   bash start.sh stop     # зупинка всього

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
IMAGE_NAME="tr23malyarchuk/pa-tr23malyarchuk:latest"
LOG_DIR="${SCRIPT_DIR}/data/logs"

mkdir -p "$LOG_DIR"

stop_all() {
    echo ""
    echo "[start] Зупиняємо систему..."
    sudo docker rm -f srv1 srv2 srv3 srv4 2>/dev/null || true
    sudo docker rm -f $(sudo docker ps -q --filter "publish=3000") 2>/dev/null || true
    sudo docker network rm lb-net 2>/dev/null || true
    kill "$(cat "$LOG_DIR/uvicorn.pid" 2>/dev/null)" 2>/dev/null || true
    echo "[start] Зупинено."
    exit 0
}

if [[ "${1:-}" == "stop" ]]; then
    stop_all
fi

trap stop_all SIGINT SIGTERM

# Крок 1: образ конвертора
echo ""
echo "[1/4] Збираємо образ конвертора..."
sudo docker build -t "$IMAGE_NAME" "$PROJECT_DIR/converter" > "$LOG_DIR/build.log" 2>&1
echo "      OK"

# Крок 2: мережа і srv1
echo "[2/4] Запускаємо srv1..."
sudo docker network create lb-net 2>/dev/null || true
sudo docker rm -f srv1 2>/dev/null || true
sudo docker run -d \
    --name srv1 \
    --network lb-net \
    --cpuset-cpus="0" \
    -p 8081:8081 \
    "$IMAGE_NAME" > /dev/null
echo "      srv1 запущено"

# Крок 3: балансировщик
echo "[3/4] Запускаємо балансировщик (main.py)..."
sudo chmod 666 /var/run/docker.sock 2>/dev/null || true
cd "$SCRIPT_DIR"
uvicorn main:app --host 0.0.0.0 --port 8000 \
    > "$LOG_DIR/uvicorn.log" 2>&1 &
echo $! > "$LOG_DIR/uvicorn.pid"
sleep 2
echo "      Балансировщик на http://localhost:8000"

# Крок 4: фронтенд
echo "[4/4] Запускаємо фронтенд..."

# Зупиняємо старий контейнер на порту 3000 якщо є
OLD=$(sudo docker ps -q --filter "publish=3000" 2>/dev/null || true)
if [[ -n "$OLD" ]]; then
    echo "      Зупиняємо старий контейнер на порту 3000..."
    sudo docker rm -f "$OLD" > /dev/null 2>&1 || true
fi

# Збираємо образ якщо немає
if ! sudo docker image inspect frontend > /dev/null 2>&1; then
    echo "      Збираємо образ frontend..."
    sudo docker build -t frontend "$PROJECT_DIR/frontend" > "$LOG_DIR/frontend_build.log" 2>&1
fi

FRONTEND_ID=$(sudo docker run -d \
    --add-host=host.docker.internal:host-gateway \
    -p 3000:80 \
    frontend)
echo "$FRONTEND_ID" > "$LOG_DIR/frontend.pid"
echo "      Фронтенд на http://localhost:3000"

echo ""
echo "══════════════════════════════════════════"
echo "  Система запущена!"
echo ""
echo "  Фронтенд:      http://localhost:3000"
echo "  Балансировщик: http://localhost:8000"
echo ""
echo "  Для демо — в окремому терміналі:"
echo "  bash scriptA.sh    (управляє контейнерами)"
echo ""
echo "  Ctrl+C — зупинити все"
echo "══════════════════════════════════════════"
echo ""

wait
