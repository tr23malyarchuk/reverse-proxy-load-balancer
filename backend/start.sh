#!/bin/bash
# start.sh — запускає всю систему
# Запускати: sudo bash start.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Визначаємо реального користувача (не root, навіть якщо запущено через sudo)
REAL_USER="${SUDO_USER:-$(logname 2>/dev/null || whoami)}"

echo "=== Збираємо образ конвертора ==="
chmod 666 /var/run/docker.sock
docker build -t tr23malyarchuk/pa-tr23malyarchuk:latest "$SCRIPT_DIR/../converter"

echo "=== Запускаємо scriptA (масштабування) ==="
bash "$SCRIPT_DIR/scriptA.sh" &
SCRIPTA_PID=$!

sleep 3

echo "=== Запускаємо балансировщик ==="
# uvicorn запускається від імені звичайного користувача,
# щоб використовувати його Python-оточення (де встановлено httpx тощо)
sudo -u "$REAL_USER" bash -c "cd '$SCRIPT_DIR' && uvicorn main:app --host 0.0.0.0 --port 8000" &
UVICORN_PID=$!

echo ""
echo "  Система запущена."
echo "  Фронтенд:         sudo docker run -p 3000:80 --add-host=host.docker.internal:host-gateway frontend"
echo "  Статус серверів:  curl -s http://localhost:8000/servers | python3 -m json.tool"
echo "  Статистика:       curl -s http://localhost:8000/stats | python3 -m json.tool"
echo "  Адмін-панель:     watch -n 2 python3 $SCRIPT_DIR/admin.py"
echo ""
echo "  Ctrl+C щоб зупинити все."

cleanup() {
    echo ""
    echo "=== Зупиняємо систему ==="
    kill "$UVICORN_PID" 2>/dev/null
    kill "$SCRIPTA_PID" 2>/dev/null
    wait "$UVICORN_PID" 2>/dev/null
    wait "$SCRIPTA_PID" 2>/dev/null
    echo "Зупинено."
}
trap cleanup SIGINT SIGTERM

wait "$UVICORN_PID"

