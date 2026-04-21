from __future__ import annotations

import asyncio
import ipaddress
import random
import sqlite3
import time
from io import BytesIO
from pathlib import Path
from typing import Callable, Dict, List, Optional

import httpx
from fastapi import (
    FastAPI,
    File,
    Form,
    HTTPException,
    Request,
    UploadFile,
)
from fastapi.responses import JSONResponse, StreamingResponse

#  Configuration and database utilities
DB_PATH = Path(__file__).parent / "requests.db"


def init_db() -> None:
    """
    Initialize the SQLite database used for storing request metrics.

    The table 'requests' is created if it does not exist.
    """
    conn = sqlite3.connect(DB_PATH)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS requests (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                algorithm   TEXT    NOT NULL,
                server_name TEXT    NOT NULL,
                endpoint    TEXT    NOT NULL,
                start_time  REAL    NOT NULL,
                end_time    REAL    NOT NULL,
                total_time  REAL    NOT NULL,
                success     INTEGER NOT NULL,
                client_ip   TEXT,
                created_at  REAL    NOT NULL
            )
            """
        )
        conn.commit()
    finally:
        conn.close()


def log_request(
    algorithm: str,
    server_name: str,
    endpoint: str,
    start_ts: float,
    end_ts: float,
    success: bool,
    client_ip: Optional[str] = None,
) -> None:
    """
    Persist a single request record in the database.

    Parameters
    ----------
    algorithm : str
        Name of the load balancing algorithm.
    server_name : str
        Identifier of the backend server that handled the request.
    endpoint : str
        Public API endpoint (e.g. '/pdf2png').
    start_ts : float
        Request start timestamp (time.time()).
    end_ts : float
        Request end timestamp (time.time()).
    success : bool
        True if the request was completed successfully, False otherwise.
    client_ip : Optional[str]
        Client IP address if available.
    """
    total_time = end_ts - start_ts

    conn = sqlite3.connect(DB_PATH)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO requests (
                algorithm,
                server_name,
                endpoint,
                start_time,
                end_time,
                total_time,
                success,
                client_ip,
                created_at
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
    finally:
        conn.close()


#  Backend server model
class BackendServer:
    """
    Representation of a single backend service instance.

    Attributes
    ----------
    name : str
        Logical name of the backend (e.g. 'pdf1').
    url : str
        Base URL of the backend (e.g. 'http://127.0.0.1:9002').
    active_connections : int
        Number of in-flight requests assigned to this backend.
    """

    def __init__(self, name: str, url: str) -> None:
        self.name = name
        self.url = url
        self.active_connections: int = 0

    def to_dict(self) -> Dict[str, object]:
        return {
            "name": self.name,
            "url": self.url,
            "active_connections": self.active_connections,
        }


# Pools for different conversion services
AUDIO_SERVERS: List[BackendServer] = [
    BackendServer("audio1", "http://127.0.0.1:9001"),
]

PDF_SERVERS: List[BackendServer] = [
    BackendServer("pdf1", "http://127.0.0.1:9002"),
]

IMAGE_SERVERS: List[BackendServer] = [
    BackendServer("img1", "http://127.0.0.1:9003"),
]

RAR_SERVERS: List[BackendServer] = [
    BackendServer("rar1", "http://127.0.0.1:9005"),
]


#  Load-balancing algorithms
_rr_index: int = 0  # global index for round-robin


def choose_round_robin(servers: List[BackendServer]) -> BackendServer:
    """
    Round-robin selection: servers are chosen in a cyclic order.
    """
    global _rr_index
    if not servers:
        raise RuntimeError("No backend servers configured")
    server = servers[_rr_index % len(servers)]
    _rr_index += 1
    return server


def choose_random(servers: List[BackendServer]) -> BackendServer:
    """
    Random selection: each request is assigned to a random backend.
    """
    if not servers:
        raise RuntimeError("No backend servers configured")
    return random.choice(servers)


def choose_least_connections(servers: List[BackendServer]) -> BackendServer:
    """
    Least-connections selection: choose the backend with minimal load.
    """
    if not servers:
        raise RuntimeError("No backend servers configured")
    return min(servers, key=lambda s: s.active_connections)


def ip_to_int(ip_str: str) -> int:
    """
    Convert a textual IP address to its integer representation.
    """
    return int(ipaddress.ip_address(ip_str))


def basic_hash(value: int) -> int:
    """
    Simple 64-bit mixing function used in the IP-hash algorithm.
    """
    value = (value ^ 0x9E3779B97F4A7C15) & ((1 << 64) - 1)
    value = (value * 0xBF58476D1CE4E5B9) & ((1 << 64) - 1)
    return value & 0xFFFFFFFFFFFFFFFF


def choose_ip_hash(servers: List[BackendServer], client_ip: str) -> BackendServer:
    """
    IP-hash selection: the same client IP is mapped to the same backend.
    """
    if not servers:
        raise RuntimeError("No backend servers configured")
    numeric_ip = ip_to_int(client_ip)
    hash_value = basic_hash(numeric_ip)
    idx = hash_value % len(servers)
    return servers[idx]


def choose_power_of_two(servers: List[BackendServer]) -> BackendServer:
    """
    Power-of-two choices algorithm.

    Two distinct backends are selected uniformly at random; the one with
    fewer active connections is chosen.
    """
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
    return server1 if server1.active_connections <= server2.active_connections else server2


def choose_backend(
    algorithm: str,
    servers: List[BackendServer],
    client_ip: Optional[str] = None,
) -> BackendServer:
    """
    Dispatch function that selects a backend according to the given algorithm.
    """
    if not servers:
        raise RuntimeError("No backend servers configured")

    if algorithm == "round_robin":
        return choose_round_robin(servers)
    if algorithm == "random":
        return choose_random(servers)
    if algorithm == "least_connections":
        return choose_least_connections(servers)
    if algorithm == "ip_hash":
        return choose_ip_hash(servers, client_ip or "127.0.0.1")
    if algorithm == "power_of_two":
        return choose_power_of_two(servers)

    raise ValueError(f"Unknown algorithm: {algorithm}")


SUPPORTED_ALGORITHMS = {
    "round_robin",
    "random",
    "least_connections",
    "ip_hash",
    "power_of_two",
}


# FastAPI application and shared HTTP client
app = FastAPI(title="Reverse Proxy Load Balancer")

http_client: httpx.AsyncClient


@app.on_event("startup")
async def on_startup() -> None:
    """
    Initialize the database and the shared HTTP client.
    """
    init_db()
    # single shared client -> connection pooling and lower overhead
    global http_client
    http_client = httpx.AsyncClient(timeout=300.0)


@app.on_event("shutdown")
async def on_shutdown() -> None:
    """
    Close shared resources on application shutdown.
    """
    await http_client.aclose()


# Auxiliary endpoints
@app.get("/servers")
async def list_servers() -> Dict[str, List[Dict[str, object]]]:
    """
    Return the current state of all backend pools.
    """
    return {
        "audio": [s.to_dict() for s in AUDIO_SERVERS],
        "pdf": [s.to_dict() for s in PDF_SERVERS],
        "image": [s.to_dict() for s in IMAGE_SERVERS],
        "archive_rar": [s.to_dict() for s in RAR_SERVERS],
    }


# Synthetic JSON endpoint for load testing
@app.post("/request")
async def handle_request(request: Request) -> Dict[str, object]:
    """
    Synthetic endpoint used in load testing.

    It emulates processing time without performing real conversion but logs
    the request in the database in the same format as real conversions.
    """
    body: Dict[str, object] = await request.json()

    algo_name = str(body.get("algorithm", "round_robin"))
    client_ip = body.get("client_ip")
    processing_time = float(body.get("processing_time", 0.1))

    if algo_name not in SUPPORTED_ALGORITHMS:
        raise HTTPException(status_code=400, detail=f"Unknown algorithm: {algo_name}")

    try:
        server = choose_backend(algo_name, AUDIO_SERVERS, str(client_ip) if client_ip else None)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Balancer error: {exc}")

    server.active_connections += 1
    start_ts = time.time()
    success = False
    error_msg: Optional[str] = None

    try:
        await asyncio.sleep(processing_time)
        success = True
    except Exception as exc:  # pragma: no cover - defensive
        error_msg = str(exc)
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
            client_ip=str(client_ip) if client_ip else None,
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


# Proxy endpoints for conversion services
async def _proxy_file_request(
    *,
    file: UploadFile,
    algorithm: str,
    client_ip: Optional[str],
    servers: List[BackendServer],
    backend_path: str,
    endpoint_name: str,
    default_content_type: str,
    response_media_type: str,
    response_filename_suffix: str,
    timeout: float,
) -> StreamingResponse | JSONResponse:
    """
    Common implementation for file-based proxy endpoints.

    This helper reduces code duplication and makes the behaviour of all
    conversion endpoints uniform.
    """
    if algorithm not in SUPPORTED_ALGORITHMS:
        raise HTTPException(status_code=400, detail=f"Unknown algorithm: {algorithm}")

    try:
        server = choose_backend(algorithm, servers, client_ip)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Balancer error: {exc}")

    file_bytes = await file.read()
    filename = file.filename or "input"

    server.active_connections += 1
    start_ts = time.time()
    success = False
    error_msg: Optional[str] = None
    result_bytes: Optional[bytes] = None

    try:
        convert_url = f"{server.url}{backend_path}"
        files = {
            "file": (
                filename,
                BytesIO(file_bytes),
                file.content_type or default_content_type,
            )
        }
        response = await http_client.post(convert_url, files=files, timeout=timeout)

        if response.status_code != 200:
            error_msg = f"{endpoint_name} service error: {response.status_code} {response.text}"
        else:
            result_bytes = response.content
            success = True
    except Exception as exc:
        error_msg = str(exc)
    finally:
        end_ts = time.time()
        server.active_connections -= 1
        log_request(
            algorithm=algorithm,
            server_name=server.name,
            endpoint=endpoint_name,
            start_ts=start_ts,
            end_ts=end_ts,
            success=success,
            client_ip=client_ip,
        )

    total_time = end_ts - start_ts

    if not success or result_bytes is None:
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
        "Content-Disposition": f'attachment; filename="{Path(filename).stem}{response_filename_suffix}"',
    }

    return StreamingResponse(
        BytesIO(result_bytes),
        media_type=response_media_type,
        headers=headers,
    )


@app.post("/file-request")
async def wav_to_mp3_request(
    file: UploadFile = File(...),
    algorithm: str = Form("round_robin"),
    client_ip: Optional[str] = Form(None),
):
    """
    Public endpoint for WAV -> MP3 conversion via the audio backend pool.
    """
    return await _proxy_file_request(
        file=file,
        algorithm=algorithm,
        client_ip=client_ip,
        servers=AUDIO_SERVERS,
        backend_path="/convert/wav-to-mp3",
        endpoint_name="/file-request",
        default_content_type="audio/wav",
        response_media_type="audio/mpeg",
        response_filename_suffix=".mp3",
        timeout=60.0,
    )


@app.post("/pdf2png")
async def pdf2png_request(
    file: UploadFile = File(...),
    algorithm: str = Form("round_robin"),
    client_ip: Optional[str] = Form(None),
):
    """
    Public endpoint for PDF -> PNG (ZIP archive) conversion.
    """
    return await _proxy_file_request(
        file=file,
        algorithm=algorithm,
        client_ip=client_ip,
        servers=PDF_SERVERS,
        backend_path="/convert/pdf-to-png",
        endpoint_name="/pdf2png",
        default_content_type="application/pdf",
        response_media_type="application/zip",
        response_filename_suffix="_pages.zip",
        timeout=120.0,
    )


@app.post("/webp2png")
async def webp2png_request(
    file: UploadFile = File(...),
    algorithm: str = Form("round_robin"),
    client_ip: Optional[str] = Form(None),
):
    """
    Public endpoint for WEBP -> PNG conversion.
    """
    return await _proxy_file_request(
        file=file,
        algorithm=algorithm,
        client_ip=client_ip,
        servers=IMAGE_SERVERS,
        backend_path="/convert/webp-to-png",
        endpoint_name="/webp2png",
        default_content_type="image/webp",
        response_media_type="image/png",
        response_filename_suffix=".png",
        timeout=60.0,
    )


@app.post("/rar2zip")
async def rar2zip_request(
    file: UploadFile = File(...),
    algorithm: str = Form("round_robin"),
    client_ip: Optional[str] = Form(None),
):
    """
    Public endpoint for RAR -> ZIP conversion.
    """
    return await _proxy_file_request(
        file=file,
        algorithm=algorithm,
        client_ip=client_ip,
        servers=RAR_SERVERS,
        backend_path="/convert/rar-to-zip",
        endpoint_name="/rar2zip",
        default_content_type="application/vnd.rar",
        response_media_type="application/zip",
        response_filename_suffix=".zip",
        timeout=300.0,
    )


@app.post("/ziprar")
async def ziprar_request(
    file: UploadFile = File(...),
    algorithm: str = Form("round_robin"),
    client_ip: Optional[str] = Form(None),
):
    """
    Public endpoint that uses the same RAR backend pool but exposes '/ziprar'
    as an alternative API name (for UI experiments).
    """
    return await _proxy_file_request(
        file=file,
        algorithm=algorithm,
        client_ip=client_ip,
        servers=RAR_SERVERS,
        backend_path="/convert/rar-to-zip",
        endpoint_name="/ziprar",
        default_content_type="application/vnd.rar",
        response_media_type="application/zip",
        response_filename_suffix=".zip",
        timeout=300.0,
    )

# uvicorn main:app --reload --port 8000
