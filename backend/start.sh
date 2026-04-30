#!/bin/bash
# start.sh — запускає всю систему

echo "=== Збираємо образ конвертора ==="
docker build -t tr23malyarchuk/pa-tr23malyarchuk:latest ../converter

echo "=== Запускаємо scriptA (масштабування) ==="
sudo bash scriptA.sh &
SCRIPTA_PID=$!

sleep 3

echo "=== Запускаємо балансировщик ==="
uvicorn main:app --host 0.0.0.0 --port 8000 &
UVICORN_PID=$!

echo ""
echo "Система запущена."
echo "Фронтенд: sudo docker run -p 3000:80 --add-host=host.docker.internal:host-gateway frontend"
echo "Статус серверів: curl -s http://localhost:8000/servers | python3 -m json.tool"
echo "Статистика: curl -s http://localhost:8000/stats | python3 -m json.tool"
echo ""
echo "Ctrl+C щоб зупинити все."

wait $UVICORN_PID
