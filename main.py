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


# --- Пулы backend-серверов под разные сервисы ---

# Аудио (WAV -> MP3)
AUDIO_SERVERS: List[BackendServer] = [
    BackendServer("audio1", "http://127.0.0.1:9001"),
    # если поднимешь дополнительные инстансы converter_wav2mp3, добавишь сюда
    # BackendServer("audio2", "http://127.0.0.1:9003"),
]

# PDF (PDF -> PNG)
PDF_SERVERS: List[BackendServer] = [
    BackendServer("pdf1", "http://127.0.0.1:9002"),
    # BackendServer("pdf2", "http://127.0.0.1:9004"),  # если захочешь масштабировать
]

IMAGE_SERVERS: List[BackendServer] = [
    BackendServer("img1", "http://127.0.0.1:9003"),
]

RAR_SERVERS: List[BackendServer] = [
    BackendServer("rar1", "http://127.0.0.1:9005"),
]

app = FastAPI(title="Reverse Proxy Load Balancer")

# --- Алгоритмы балансировки (общие, работают с любым пулом servers) ---

_rr_index = 0


def choose_round_robin(servers: List[BackendServer]) -> BackendServer:
    global _rr_index
    if not servers:
        raise RuntimeError("No backend servers configured")
    server = servers[_rr_index % len(servers)]
    _rr_index += 1
    return server


def choose_random(servers: List[BackendServer]) -> BackendServer:
    if not servers:
        raise RuntimeError("No backend servers configured")
    return random.choice(servers)


def choose_least_connections(servers: List[BackendServer]) -> BackendServer:
    if not servers:
        raise RuntimeError("No backend servers configured")
    return min(servers, key=lambda s: s.active_connections)


def ip_to_int(ip_str: str) -> int:
    return int(ipaddress.ip_address(ip_str))


def basic_hash(value: int) -> int:
    value = (value ^ 0x9e3779b97f4a7c15) & ((1 << 64) - 1)
    value = (value * 0xbf58476d1ce4e5b9) & ((1 << 64) - 1)
    return value & 0xFFFFFFFFFFFFFFFF


def choose_ip_hash(servers: List[BackendServer], client_ip: str) -> BackendServer:
    if not servers:
        raise RuntimeError("No backend servers configured")
    numeric_ip = ip_to_int(client_ip)
    hash_value = basic_hash(numeric_ip)
    idx = hash_value % len(servers)
    return servers[idx]


def choose_power_of_two(servers: List[BackendServer]) -> BackendServer:
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


# Обёртки алгоритмов: принимают пул servers и client_ip
ALGORITHMS: Dict[str, Callable[..., BackendServer]] = {
    "round_robin": lambda servers, client_ip=None: choose_round_robin(servers),
    "random": lambda servers, client_ip=None: choose_random(servers),
    "least_connections": lambda servers, client_ip=None: choose_least_connections(
        servers
    ),
    "ip_hash": lambda servers, client_ip=None: choose_ip_hash(
        servers, client_ip or "127.0.0.1"
    ),
    "power_of_two": lambda servers, client_ip=None: choose_power_of_two(servers),
}


@app.get("/servers")
async def list_servers():
    """
    Возвращаем состояние всех пулов backend-серверов.
    Это удобно и для отладки, и как иллюстрация в дипломе.
    """
    return {
        "audio": [s.to_dict() for s in AUDIO_SERVERS],
        "pdf": [s.to_dict() for s in PDF_SERVERS],
        "image": [s.to_dict() for s in IMAGE_SERVERS],
        "archive_rar": [s.to_dict() for s in RAR_SERVERS],
    }


# --- Синтетический JSON-эндпоинт для нагрузочных тестов ---

@app.post("/request")
async def handle_request(request: Request):
    """
    Синтетический эндпоинт: имитирует обработку без реальной конвертации,
    но логирует метрики в БД (таблица requests).

    Для простоты используем пул AUDIO_SERVERS,
    но при желании можно сделать отдельный пул для синтетики.
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
        server = ALGORITHMS[algo_name](servers=AUDIO_SERVERS, client_ip=client_ip)
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


# --- Эндпоинт для WAV->MP3: прокси на converter_wav2mp3 + логирование ---

@app.post("/file-request")
async def file_request(
    file: UploadFile = File(...),
    algorithm: str = Form("round_robin"),
    client_ip: Optional[str] = Form(None),
):
    """
    Эндпоинт, принимающий WAV-файл и поле algorithm, выбирающий backend
    из пула AUDIO_SERVERS и проксирующий запрос на /convert/wav-to-mp3.
    Логирует метрики в SQLite (таблица requests).
    """
    if algorithm not in ALGORITHMS:
        raise HTTPException(status_code=400, detail=f"Unknown algorithm: {algorithm}")

    try:
        server = ALGORITHMS[algorithm](servers=AUDIO_SERVERS, client_ip=client_ip)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Balancer error: {e}")

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
            detail={
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
        "Content-Disposition": f'attachment; filename="{Path(filename).stem}.mp3"',
    }

    return StreamingResponse(
        BytesIO(mp3_bytes),
        media_type="audio/mpeg",
        headers=headers,
    )


# --- Эндпоинт для PDF->PNG: прокси на converter_pdf2png + логирование ---

@app.post("/pdf2png")
async def pdf2png_request(
    file: UploadFile = File(...),
    algorithm: str = Form("round_robin"),
    client_ip: Optional[str] = Form(None),
):
    """
    Эндпоинт для конвертации PDF → PNG через отдельный сервис.
    Принимает PDF и algorithm, выбирает backend из пула PDF_SERVERS,
    проксирует запрос на /convert/pdf-to-png и логирует метрики в SQLite.
    Возвращает ZIP с PNG-страницами.
    """
    if algorithm not in ALGORITHMS:
        return JSONResponse(
            status_code=400,
            content={"error": f"Unknown algorithm: {algorithm}"},
        )

    try:
        server = ALGORITHMS[algorithm](servers=PDF_SERVERS, client_ip=client_ip)
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"error": str(e)},
        )

    file_bytes = await file.read()
    filename = file.filename or "input.pdf"

    server.active_connections += 1
    start_ts = time.time()

    try:
        convert_url = f"{server.url}/convert/pdf-to-png"

        async with httpx.AsyncClient() as client:
            files = {
                "file": (
                    filename,
                    BytesIO(file_bytes),
                    file.content_type or "application/pdf",
                )
            }
            resp = await client.post(convert_url, files=files, timeout=120.0)

        if resp.status_code != 200:
            end_ts = time.time()
            server.active_connections -= 1
            log_request(
                algorithm=algorithm,
                server_name=server.name,
                endpoint="/pdf2png",
                start_ts=start_ts,
                end_ts=end_ts,
                success=False,
                client_ip=client_ip,
            )
            return JSONResponse(
                status_code=resp.status_code,
                content={
                    "error": "PDF2PNG service error",
                    "details": resp.text,
                    "chosen_server": server.to_dict(),
                },
            )

        zip_bytes = resp.content
        success = True
        error_msg = None
    except Exception as e:
        zip_bytes = None
        success = False
        error_msg = str(e)
    finally:
        end_ts = time.time()
        server.active_connections -= 1
        log_request(
            algorithm=algorithm,
            server_name=server.name,
            endpoint="/pdf2png",
            start_ts=start_ts,
            end_ts=end_ts,
            success=success,
            client_ip=client_ip,
        )

    total_time = end_ts - start_ts

    if not success or zip_bytes is None:
        return JSONResponse(
            status_code=500,
            content={
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
        "Content-Disposition": f'attachment; filename="{Path(filename).stem}_pages.zip"',
    }

    return StreamingResponse(
        BytesIO(zip_bytes),
        media_type="application/zip",
        headers=headers,
    )

@app.post("/webp2png")
async def webp2png_request(
    file: UploadFile = File(...),
    algorithm: str = Form("round_robin"),
    client_ip: Optional[str] = Form(None),
):
    if algorithm not in ALGORITHMS:
        return JSONResponse(
            status_code=400,
            content={"error": f"Unknown algorithm: {algorithm}"},
        )

    try:
        server = ALGORITHMS[algorithm](servers=IMAGE_SERVERS, client_ip=client_ip)
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"error": str(e)},
        )

    file_bytes = await file.read()
    filename = file.filename or "input.webp"

    server.active_connections += 1
    start_ts = time.time()

    try:
        convert_url = f"{server.url}/convert/webp-to-png"

        async with httpx.AsyncClient() as client:
            files = {
                "file": (
                    filename,
                    BytesIO(file_bytes),
                    file.content_type or "image/webp",
                )
            }
            resp = await client.post(convert_url, files=files, timeout=60.0)

        if resp.status_code != 200:
            end_ts = time.time()
            server.active_connections -= 1
            log_request(
                algorithm=algorithm,
                server_name=server.name,
                endpoint="/webp2png",
                start_ts=start_ts,
                end_ts=end_ts,
                success=False,
                client_ip=client_ip,
            )
            return JSONResponse(
                status_code=resp.status_code,
                content={
                    "error": "WEBP2PNG service error",
                    "details": resp.text,
                    "chosen_server": server.to_dict(),
                },
            )

        png_bytes = resp.content
        success = True
        error_msg = None
    except Exception as e:
        png_bytes = None
        success = False
        error_msg = str(e)
    finally:
        end_ts = time.time()
        server.active_connections -= 1
        log_request(
            algorithm=algorithm,
            server_name=server.name,
            endpoint="/webp2png",
            start_ts=start_ts,
            end_ts=end_ts,
            success=success,
            client_ip=client_ip,
        )

    total_time = end_ts - start_ts

    if not success or png_bytes is None:
        return JSONResponse(
            status_code=500,
            content={
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
        "Content-Disposition": f'attachment; filename="{Path(filename).stem}.png"',
    }

    return StreamingResponse(
        BytesIO(png_bytes),
        media_type="image/png",
        headers=headers,
    )

@app.post("/rar2zip")
async def rar2zip_request(
    file: UploadFile = File(...),
    algorithm: str = Form("round_robin"),
    client_ip: Optional[str] = Form(None),
):
    if algorithm not in ALGORITHMS:
        return JSONResponse(
            status_code=400,
            content={"error": f"Unknown algorithm: {algorithm}"},
        )

    try:
        server = ALGORITHMS[algorithm](servers=RAR_SERVERS, client_ip=client_ip)
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"error": str(e)},
        )

    file_bytes = await file.read()
    filename = file.filename or "input.rar"

    server.active_connections += 1
    start_ts = time.time()

    try:
        convert_url = f"{server.url}/convert/rar-to-zip"

        async with httpx.AsyncClient() as client:
            files = {
                "file": (
                    filename,
                    BytesIO(file_bytes),
                    file.content_type or "application/vnd.rar",
                )
            }
            resp = await client.post(convert_url, files=files, timeout=300.0)

        if resp.status_code != 200:
            end_ts = time.time()
            server.active_connections -= 1
            log_request(
                algorithm=algorithm,
                server_name=server.name,
                endpoint="/rar2zip",
                start_ts=start_ts,
                end_ts=end_ts,
                success=False,
                client_ip=client_ip,
            )
            return JSONResponse(
                status_code=resp.status_code,
                content={
                    "error": "RAR2ZIP service error",
                    "details": resp.text,
                    "chosen_server": server.to_dict(),
                },
            )

        zip_bytes = resp.content
        success = True
        error_msg = None
    except Exception as e:
        zip_bytes = None
        success = False
        error_msg = str(e)
    finally:
        end_ts = time.time()
        server.active_connections -= 1
        log_request(
            algorithm=algorithm,
            server_name=server.name,
            endpoint="/rar2zip",
            start_ts=start_ts,
            end_ts=end_ts,
            success=success,
            client_ip=client_ip,
        )

    total_time = end_ts - start_ts

    if not success or zip_bytes is None:
        return JSONResponse(
            status_code=500,
            content={
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
        "Content-Disposition": f'attachment; filename="{Path(filename).stem}.zip"',
    }

    return StreamingResponse(
        BytesIO(zip_bytes),
        media_type="application/zip",
        headers=headers,
    )

@app.post("/ziprar")
async def ziprar_request(
    file: UploadFile = File(...),
    algorithm: str = Form("round_robin"),
    client_ip: Optional[str] = Form(None),
):
    if algorithm not in ALGORITHMS:
        return JSONResponse(
            status_code=400,
            content={"error": f"Unknown algorithm: {algorithm}"},
        )

    try:
        server = ALGORITHMS[algorithm](servers=RAR_SERVERS, client_ip=client_ip)
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"error": str(e)},
        )

    file_bytes = await file.read()
    filename = file.filename or "input.rar"

    server.active_connections += 1
    start_ts = time.time()

    try:
        convert_url = f"{server.url}/convert/rar-to-zip"

        async with httpx.AsyncClient() as client:
            files = {
                "file": (
                    filename,
                    BytesIO(file_bytes),
                    file.content_type or "application/vnd.rar",
                )
            }
            resp = await client.post(convert_url, files=files, timeout=300.0)

        if resp.status_code != 200:
            end_ts = time.time()
            server.active_connections -= 1
            log_request(
                algorithm=algorithm,
                server_name=server.name,
                endpoint="/ziprar",
                start_ts=start_ts,
                end_ts=end_ts,
                success=False,
                client_ip=client_ip,
            )
            return JSONResponse(
                status_code=resp.status_code,
                content={
                    "error": "RAR2ZIP service error",
                    "details": resp.text,
                    "chosen_server": server.to_dict(),
                },
            )

        zip_bytes = resp.content
        success = True
        error_msg = None
    except Exception as e:
        zip_bytes = None
        success = False
        error_msg = str(e)
    finally:
        end_ts = time.time()
        server.active_connections -= 1
        log_request(
            algorithm=algorithm,
            server_name=server.name,
            endpoint="/ziprar",
            start_ts=start_ts,
            end_ts=end_ts,
            success=success,
            client_ip=client_ip,
        )

    total_time = end_ts - start_ts

    if not success or zip_bytes is None:
        return JSONResponse(
            status_code=500,
            content={
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
        "Content-Disposition": f'attachment; filename="{Path(filename).stem}.zip"',
    }

    return StreamingResponse(
        BytesIO(zip_bytes),
        media_type="application/zip",
        headers=headers,
    )

# Запуск:
# uvicorn main:app --reload --port 8000

