from __future__ import annotations

import asyncio
import ipaddress
import random
import sqlite3
import subprocess
import time
from io import BytesIO
from pathlib import Path
from typing import Dict, List, Optional

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


# ---------------------------------------------------------------------------
# Configuration and database utilities
# ---------------------------------------------------------------------------

DB_PATH = Path(__file__).parent / "data/requests.db"


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


# ---------------------------------------------------------------------------
# Backend server model
# ---------------------------------------------------------------------------

class BackendServer:
    """
    Representation of a single backend service instance.

    Attributes
    ----------
    name : str
        Logical name of the backend (e.g. 'srv1').
    url : str
        Base URL of the backend (e.g. 'http://127.0.0.1:8081').
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


# ---------------------------------------------------------------------------
# Static pools for non-audio services (no dynamic scaling needed there)
# ---------------------------------------------------------------------------

PDF_SERVERS: List[BackendServer] = [
    BackendServer("pdf1", "http://127.0.0.1:9002"),
]

IMAGE_SERVERS: List[BackendServer] = [
    BackendServer("img1", "http://127.0.0.1:9003"),
]

RAR_SERVERS: List[BackendServer] = [
    BackendServer("rar1", "http://127.0.0.1:9005"),
]

# Audio pool — starts with one server; DockerPoolManager grows/shrinks it.
AUDIO_SERVERS: List[BackendServer] = [
    BackendServer("srv1", "http://127.0.0.1:8081"),
]


# ---------------------------------------------------------------------------
# Docker pool manager (mirrors scriptA.sh logic in Python)
# ---------------------------------------------------------------------------

# Mirrors the container topology from scriptA.sh
_DOCKER_SLOTS: List[Dict[str, object]] = [
    {"name": "srv1", "port": 8081, "cpu": "0"},
    {"name": "srv2", "port": 8082, "cpu": "1"},
    {"name": "srv3", "port": 8083, "cpu": "2"},
    {"name": "srv4", "port": 8084, "cpu": "3"},
]

_DOCKER_IMAGE = "tr23malyarchuk/pa-tr23malyarchuk:latest"

# Thresholds (in monitor ticks, one tick = 60 s) — same as scriptA.sh
_BUSY_THRESHOLD = 2   # consecutive busy ticks  → scale up
_IDLE_THRESHOLD = 2   # consecutive idle ticks  → scale down
_MONITOR_INTERVAL = 60  # seconds between CPU checks


def _docker_run(name: str, cpu: str, port: int) -> None:
    """Start a named container on the given CPU core and host port."""
    # Remove stale container if present (like cleanup_container_by_name)
    subprocess.run(
        ["docker", "rm", "-f", name],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    subprocess.run(
        [
            "docker", "run", "-d",
            "--name", name,
            f"--cpuset-cpus={cpu}",
            "-p", f"{port}:8081",
            _DOCKER_IMAGE,
        ],
        check=True,
    )
    print(f"[pool] Started container {name} on CPU {cpu}, port {port}")


def _docker_stop(name: str) -> None:
    """Stop and remove a container."""
    subprocess.run(
        ["docker", "rm", "-f", name],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    print(f"[pool] Removed container {name}")


def _docker_cpu_percent(name: str) -> float:
    """
    Return the current CPU usage (%) of a running container.
    Returns 0.0 if the container is not running or stats are unavailable.
    """
    result = subprocess.run(
        ["docker", "stats", "--no-stream", "--format", "{{.CPUPerc}}", name],
        capture_output=True,
        text=True,
    )
    raw = result.stdout.strip().replace("%", "")
    try:
        return float(raw)
    except ValueError:
        return 0.0


def _docker_image_updated() -> bool:
    """
    Pull the Docker image and return True if a newer version was downloaded.
    Mirrors check_for_image_update in scriptA.sh.
    """
    result = subprocess.run(
        ["docker", "pull", _DOCKER_IMAGE],
        capture_output=True,
        text=True,
    )
    return "Downloaded newer image" in result.stdout


def _pool_add_server(slot: Dict[str, object]) -> None:
    """
    Launch a container for *slot* and register it in AUDIO_SERVERS.
    No-op if a server with that name already exists in the pool.
    """
    name = str(slot["name"])
    if any(s.name == name for s in AUDIO_SERVERS):
        return
    port = int(str(slot["port"]))
    cpu = str(slot["cpu"])
    _docker_run(name, cpu, port)
    AUDIO_SERVERS.append(BackendServer(name, f"http://127.0.0.1:{port}"))
    print(f"[pool] Added {name} to AUDIO_SERVERS (total={len(AUDIO_SERVERS)})")


def _pool_remove_server(name: str) -> None:
    """
    Stop the Docker container for *name* and remove it from AUDIO_SERVERS.
    The first server (srv1) is never removed.
    """
    global AUDIO_SERVERS
    AUDIO_SERVERS = [s for s in AUDIO_SERVERS if s.name != name]
    _docker_stop(name)
    print(f"[pool] Removed {name} from AUDIO_SERVERS (total={len(AUDIO_SERVERS)})")


def _rolling_update() -> None:
    """
    Restart every running container one-by-one with the new image,
    keeping at least one instance alive at all times.
    Mirrors update_all_containers in scriptA.sh.
    """
    running = [s.name for s in AUDIO_SERVERS]
    if not running:
        return

    # Keep the first container accessible while updating the rest
    accessible = running[0]
    for name in running[1:]:
        slot = next((s for s in _DOCKER_SLOTS if s["name"] == name), None)
        if slot is None:
            continue
        print(f"[pool] Rolling-update: restarting {name}")
        _docker_run(name, str(slot["cpu"]), int(str(slot["port"])))

    # Finally restart the one we kept alive
    slot = next((s for s in _DOCKER_SLOTS if s["name"] == accessible), None)
    if slot:
        print(f"[pool] Rolling-update: restarting accessible container {accessible}")
        _docker_run(accessible, str(slot["cpu"]), int(str(slot["port"])))

    print("[pool] Rolling update complete")


async def _docker_pool_monitor() -> None:
    """
    Background task that replicates scriptA.sh's monitor_container_busy loop.

    Every _MONITOR_INTERVAL seconds it:
      1. Checks for a newer Docker image → rolling update if found.
      2. Reads CPU usage of the *most-loaded* active container.
      3. Increments busy_count or idle_count accordingly.
      4. Scales up (add next slot) when busy_count >= _BUSY_THRESHOLD.
      5. Scales down (remove last slot) when idle_count >= _IDLE_THRESHOLD
         and more than one server is running.
    """
    busy_count = 0
    idle_count = 0

    while True:
        await asyncio.sleep(_MONITOR_INTERVAL)

        # --- image update check (runs in thread-pool to avoid blocking) ---
        try:
            updated = await asyncio.get_event_loop().run_in_executor(
                None, _docker_image_updated
            )
            if updated:
                print("[pool] New image detected — starting rolling update")
                await asyncio.get_event_loop().run_in_executor(None, _rolling_update)
                busy_count = 0
                idle_count = 0
                continue
        except Exception as exc:
            print(f"[pool] Image check failed: {exc}")

        # --- CPU monitoring ---
        if not AUDIO_SERVERS:
            continue

        # Use the most-loaded server as the indicator (conservative approach)
        cpu_values: List[float] = []
        for server in list(AUDIO_SERVERS):
            try:
                pct = await asyncio.get_event_loop().run_in_executor(
                    None, _docker_cpu_percent, server.name
                )
                cpu_values.append(pct)
                print(f"[pool] {server.name} CPU={pct:.1f}%")
            except Exception as exc:
                print(f"[pool] Could not read stats for {server.name}: {exc}")

        if not cpu_values:
            continue

        max_cpu = max(cpu_values)

        if max_cpu > 0.0:
            busy_count += 1
            idle_count = 0
            print(f"[pool] Busy tick {busy_count}/{_BUSY_THRESHOLD} (max CPU={max_cpu:.1f}%)")
        else:
            idle_count += 1
            busy_count = 0
            print(f"[pool] Idle tick {idle_count}/{_IDLE_THRESHOLD}")

        # --- scale up ---
        if busy_count >= _BUSY_THRESHOLD:
            current_names = {s.name for s in AUDIO_SERVERS}
            next_slot = next(
                (sl for sl in _DOCKER_SLOTS if sl["name"] not in current_names),
                None,
            )
            if next_slot:
                print(f"[pool] Scaling up → adding {next_slot['name']}")
                try:
                    await asyncio.get_event_loop().run_in_executor(
                        None, _pool_add_server, next_slot
                    )
                except Exception as exc:
                    print(f"[pool] Scale-up failed: {exc}")
            else:
                print("[pool] Already at maximum capacity (4 servers)")
            busy_count = 0

        # --- scale down ---
        elif idle_count >= _IDLE_THRESHOLD and len(AUDIO_SERVERS) > 1:
            # Remove the last-added server (highest index in _DOCKER_SLOTS)
            current_names = [s.name for s in AUDIO_SERVERS]
            # Find the last slot that is currently active
            last_name = next(
                (sl["name"] for sl in reversed(_DOCKER_SLOTS) if sl["name"] in current_names),
                None,
            )
            if last_name and last_name != "srv1":
                print(f"[pool] Scaling down → removing {last_name}")
                try:
                    await asyncio.get_event_loop().run_in_executor(
                        None, _pool_remove_server, last_name
                    )
                except Exception as exc:
                    print(f"[pool] Scale-down failed: {exc}")
            idle_count = 0


# ---------------------------------------------------------------------------
# Load-balancing algorithms
# ---------------------------------------------------------------------------

_rr_index: int = 0  # global index for round-robin


def choose_round_robin(servers: List[BackendServer]) -> BackendServer:
    """Round-robin selection: servers are chosen in a cyclic order."""
    global _rr_index
    if not servers:
        raise RuntimeError("No backend servers configured")
    server = servers[_rr_index % len(servers)]
    _rr_index += 1
    return server


def choose_random(servers: List[BackendServer]) -> BackendServer:
    """Random selection: each request is assigned to a random backend."""
    if not servers:
        raise RuntimeError("No backend servers configured")
    return random.choice(servers)


def choose_least_connections(servers: List[BackendServer]) -> BackendServer:
    """Least-connections selection: choose the backend with minimal load."""
    if not servers:
        raise RuntimeError("No backend servers configured")
    return min(servers, key=lambda s: s.active_connections)


def ip_to_int(ip_str: str) -> int:
    """Convert a textual IP address to its integer representation."""
    return int(ipaddress.ip_address(ip_str))


def basic_hash(value: int) -> int:
    """Simple 64-bit mixing function used in the IP-hash algorithm."""
    value = (value ^ 0x9E3779B97F4A7C15) & ((1 << 64) - 1)
    value = (value * 0xBF58476D1CE4E5B9) & ((1 << 64) - 1)
    return value & 0xFFFFFFFFFFFFFFFF


def choose_ip_hash(servers: List[BackendServer], client_ip: str) -> BackendServer:
    """IP-hash selection: the same client IP is mapped to the same backend."""
    if not servers:
        raise RuntimeError("No backend servers configured")
    numeric_ip = ip_to_int(client_ip)
    hash_value = basic_hash(numeric_ip)
    idx = hash_value % len(servers)
    return servers[idx]


def choose_power_of_two(servers: List[BackendServer]) -> BackendServer:
    """
    Power-of-two choices: pick two random backends, route to the less loaded.
    """
    if not servers:
        raise RuntimeError("No backend servers configured")

    if len(servers) == 1:
        return servers[0]

    index1 = random.randint(0, len(servers) - 1)
    index2 = random.randint(0, len(servers) - 1)
    while index1 == index2:
        index2 = random.randint(0, len(servers) - 1)

    s1, s2 = servers[index1], servers[index2]
    return s1 if s1.active_connections <= s2.active_connections else s2


def choose_backend(
    algorithm: str,
    servers: List[BackendServer],
    client_ip: Optional[str] = None,
) -> BackendServer:
    """Dispatch function that selects a backend according to the given algorithm."""
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


# ---------------------------------------------------------------------------
# FastAPI application and shared HTTP client
# ---------------------------------------------------------------------------

app = FastAPI(title="Reverse Proxy Load Balancer")

http_client: httpx.AsyncClient


@app.on_event("startup")
async def on_startup() -> None:
    """Initialize the database, the shared HTTP client, and start the pool monitor."""
    init_db()
    global http_client
    http_client = httpx.AsyncClient(timeout=300.0)

    # Launch first container (srv1) if Docker is available
    try:
        first_slot = _DOCKER_SLOTS[0]
        await asyncio.get_event_loop().run_in_executor(
            None,
            lambda: _docker_run(
                str(first_slot["name"]),
                str(first_slot["cpu"]),
                int(str(first_slot["port"])),
            ),
        )
    except Exception as exc:
        print(f"[pool] Could not start initial container (Docker unavailable?): {exc}")

    # Start the background monitor task (non-blocking)
    asyncio.create_task(_docker_pool_monitor())


@app.on_event("shutdown")
async def on_shutdown() -> None:
    """Close shared resources on application shutdown."""
    await http_client.aclose()


# ---------------------------------------------------------------------------
# Auxiliary endpoints
# ---------------------------------------------------------------------------

@app.get("/servers")
async def list_servers() -> Dict[str, List[Dict[str, object]]]:
    """Return the current state of all backend pools."""
    return {
        "audio": [s.to_dict() for s in AUDIO_SERVERS],
        "pdf": [s.to_dict() for s in PDF_SERVERS],
        "image": [s.to_dict() for s in IMAGE_SERVERS],
        "archive_rar": [s.to_dict() for s in RAR_SERVERS],
    }


# ---------------------------------------------------------------------------
# Synthetic JSON endpoint for load testing
# ---------------------------------------------------------------------------

@app.post("/request")
async def handle_request(request: Request) -> Dict[str, object]:
    """
    Synthetic endpoint used in load testing.

    Emulates processing time without performing real conversion but logs
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
    except Exception as exc:  # pragma: no cover
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


# ---------------------------------------------------------------------------
# Proxy endpoints for conversion services
# ---------------------------------------------------------------------------

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

    Reduces code duplication and keeps the behaviour of all conversion
    endpoints uniform.
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
        "Content-Disposition": (
            f'attachment; filename="{Path(filename).stem}{response_filename_suffix}"'
        ),
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
    """Public endpoint for WAV -> MP3 conversion via the audio backend pool."""
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
    """Public endpoint for PDF -> PNG (ZIP archive) conversion."""
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
    """Public endpoint for WEBP -> PNG conversion."""
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
    """Public endpoint for RAR -> ZIP conversion."""
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