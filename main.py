from fastapi import FastAPI, Request, UploadFile, File, Form, HTTPException
from fastapi.responses import JSONResponse, StreamingResponse
import random
import ipaddress
import asyncio
from typing import List, Dict, Callable, Optional
import httpx
import time
from io import BytesIO
import sqlite3
from pathlib import Path

# --- Настройки БД (SQLite) ---

DB_PATH = Path(__file__).parent / "requests.db"


def init_db():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS requests (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            algorithm TEXT NOT NULL,
            server_name TEXT NOT NULL,
            endpoint TEXT NOT NULL,
            start_time REAL NOT NULL,
            end_time REAL NOT NULL,
            total_time REAL NOT NULL,
            success INTEGER NOT NULL,
            client_ip TEXT,
            created_at REAL NOT NULL
        )
        """
    )
    conn.commit()
    conn.close()


def log_request(
    algorithm: str,
    server_name: str,
    endpoint: str,
    start_ts: float,
    end_ts: float,
    success: bool,
    client_ip: Optional[str] = None,
):
    total_time = end_ts - start_ts
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO requests (
            algorithm, server_name, endpoint,
            start_time, end_time, total_time,
            success, client_ip, created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            algorithm,
            server_name,
            endpoint,
            start_ts,
            end_ts,
            total_time,
            1 if success else 0,
            client_ip,
            time.time(),
        ),
    )
    conn.commit()
    conn.close()


# Инициализация БД при старте
init_db()

# --- Модель backend-серверов ---

class BackendServer:
    def __init__(self, name: str, url: str):
        self.name = name
        # Базовый URL сервиса конвертации, например http://127.0.0.1:9001
        self.url = url
        self.active_connections = 0

    def to_dict(self):
        return {
            "name": self.name,
            "url": self.url,
            "active_connections": self.active_connections,
        }


# Здесь считаем, что у нас три инстанса converter_wav2mp3
servers: List[BackendServer] = [
    BackendServer("srv1", "http://127.0.0.1:9001"),
    BackendServer("srv2", "http://127.0.0.1:9002"),
    BackendServer("srv3", "http://127.0.0.1:9003"),
]

app = FastAPI(title="Reverse Proxy Load Balancer")

# --- Алгоритмы балансировки ---

_rr_index = 0


def choose_round_robin() -> BackendServer:
    global _rr_index
    if not servers:
        raise RuntimeError("No backend servers configured")
    server = servers[_rr_index % len(servers)]
    _rr_index += 1
    return server


def choose_random() -> BackendServer:
    if not servers:
        raise RuntimeError("No backend servers configured")
    return random.choice(servers)


def choose_least_connections() -> BackendServer:
    if not servers:
        raise RuntimeError("No backend servers configured")
    return min(servers, key=lambda s: s.active_connections)


def ip_to_int(ip_str: str) -> int:
    return int(ipaddress.ip_address(ip_str))


def basic_hash(value: int) -> int:
    value = (value ^ 0x9e3779b97f4a7c15) & ((1 << 64) - 1)
    value = (value * 0xbf58476d1ce4e5b9) & ((1 << 64) - 1)
    return value & 0xFFFFFFFFFFFFFFFF


def choose_ip_hash(client_ip: str) -> BackendServer:
    if not servers:
        raise RuntimeError("No backend servers configured")
    numeric_ip = ip_to_int(client_ip)
    hash_value = basic_hash(numeric_ip)
    idx = hash_value % len(servers)
    return servers[idx]


def choose_power_of_two() -> BackendServer:
    if not servers:
        raise RuntimeError("No backend servers configured")

    if len(servers) == 1:
        return servers[0]

    index1 = random.randint(0, len(servers) - 1)
    index2 = random.randint(0, len(servers) - 1)
    while index1 == index2:
        index2 = random.randint(0, len(servers) - 1)

    server1 = servers[index1]
    server2 = servers[index2]

    if server1.active_connections <= server2.active_connections:
        return server1
    else:
        return server2


ALGORITHMS: Dict[str, Callable[..., BackendServer]] = {
    "round_robin": lambda **kwargs: choose_round_robin(),
    "random": lambda **kwargs: choose_random(),
    "least_connections": lambda **kwargs: choose_least_connections(),
    "ip_hash": lambda client_ip=None, **kwargs: choose_ip_hash(
        client_ip or "127.0.0.1"
    ),
    "power_of_two": lambda **kwargs: choose_power_of_two(),
}


@app.get("/servers")
async def list_servers():
    return [s.to_dict() for s in servers]


# --- Синтетический JSON-эндпоинт для нагрузочных тестов ---

@app.post("/request")
async def handle_request(request: Request):
    """
    Синтетический эндпоинт: имитирует обработку без реальной конвертации,
    но логирует метрики в БД (таблица requests).
    """
    body: Dict = await request.json()

    algo_name: str = body.get("algorithm", "round_robin")
    client_ip: Optional[str] = body.get("client_ip")
    processing_time = float(body.get("processing_time", 0.1))

    if algo_name not in ALGORITHMS:
        return JSONResponse(
            status_code=400,
            content={"error": f"Unknown algorithm: {algo_name}"},
        )

    try:
        server = ALGORITHMS[algo_name](client_ip=client_ip)
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"error": str(e)},
        )

    server.active_connections += 1
    start_ts = time.time()

    try:
        await asyncio.sleep(processing_time)
        success = True
        error_msg = None
    except Exception as e:
        success = False
        error_msg = str(e)
    finally:
        end_ts = time.time()
        server.active_connections -= 1
        # логируем в БД
        log_request(
            algorithm=algo_name,
            server_name=server.name,
            endpoint="/request",
            start_ts=start_ts,
            end_ts=end_ts,
            success=success,
            client_ip=client_ip,
        )

    total_time = end_ts - start_ts

    return {
        "chosen_server": server.to_dict(),
        "algorithm": algo_name,
        "client_ip": client_ip,
        "success": success,
        "error": error_msg,
        "processing_time_param": processing_time,
        "total_time": total_time,
    }


# --- Новый эндпоинт для файлов: прокси на converter_wav2mp3 + логирование ---

@app.post("/file-request")
async def file_request(
    file: UploadFile = File(...),
    algorithm: str = Form("round_robin"),
    client_ip: Optional[str] = Form(None),
):
    """
    Эндпоинт, принимающий WAV-файл и поле algorithm, выбирающий backend
    и проксирующий запрос на /convert/wav-to-mp3 выбранного сервера.
    Логирует метрики в SQLite (таблица requests).
    """
    if algorithm not in ALGORITHMS:
        raise HTTPException(status_code=400,
                detail=f"Unknown algorithm: {algorithm}")

    try:
        server = ALGORITHMS[algorithm](client_ip=client_ip)
    except Exception as e:
        raise HTTPException(status_code=500,
                detail=f"Balancer error: {e}")

    file_bytes = await file.read()
    filename = file.filename or "input.wav"

    server.active_connections += 1
    start_ts = time.time()
    mp3_bytes = None
    success = False
    error_msg = None

    try:
        convert_url = f"{server.url}/convert/wav-to-mp3"

        async with httpx.AsyncClient() as client:
            files = {
                "file": (filename, BytesIO(file_bytes), file.content_type or "audio/wav")
            }
            resp = await client.post(convert_url, files=files, timeout=60.0)

        if resp.status_code != 200:
            error_msg = f"Conversion service error: {resp.status_code} {resp.text}"
        else:
            mp3_bytes = resp.content
            success = True

    except Exception as e:
        error_msg = str(e)
    finally:
        end_ts = time.time()
        server.active_connections -= 1
        # логируем в БД
        log_request(
            algorithm=algorithm,
            server_name=server.name,
            endpoint="/file-request",
            start_ts=start_ts,
            end_ts=end_ts,
            success=success,
            client_ip=client_ip,
        )

    total_time = end_ts - start_ts

    if not success or mp3_bytes is None:
        raise HTTPException(
                status_code=500,
                detail= {
                    "error": error_msg or "Unknown error",
                    "chosen_server": server.to_dict(),
                    "algorithm": algorithm,
                    "total_time": total_time,
                },
        )

    headers = {
        "X-Chosen-Server": server.name,
        "X-Algorithm": algorithm,
        "X-Total-Time": str(total_time),
        "Content-Disposition": f'attachment; filename="{Path(filename).stem}.mp3"'
    }

    return StreamingResponse(
        BytesIO(mp3_bytes),
        media_type="audio/mpeg",
        headers=headers,
    )

# Запуск:
# uvicorn main:app --reload --port 8000

