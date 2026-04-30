"""
Reverse-Proxy Load Balancer
============================
Dynamically discovers backend containers (srv1-srv4) via the Docker socket
and distributes conversion requests across all healthy instances.

scriptA.sh is responsible for starting / stopping containers.
This process just watches what is running and routes accordingly.

Run:
    uvicorn main:app --reload --port 8000
"""

from __future__ import annotations

import asyncio
import ipaddress
import random
import sqlite3
import time
import urllib.parse
from io import BytesIO
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import httpx
from fastapi import FastAPI, File, Form, HTTPException, Request, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DB_PATH = Path(__file__).parent / "data/requests.db"

# Containers managed by scriptA.sh  →  their host ports
MANAGED_CONTAINERS: Dict[str, int] = {
    "srv1": 8081,
    "srv2": 8082,
    "srv3": 8083,
    "srv4": 8084,
}

# How often (seconds) to refresh the list of live containers
POOL_REFRESH_INTERVAL: float = 10.0

# How many times to retry a request on a different server before giving up
MAX_RETRIES: int = 3

# Seconds to wait between retry attempts when no healthy server is available yet
RETRY_WAIT: float = 3.0

SUPPORTED_ALGORITHMS = {
    "round_robin",
    "random",
    "least_connections",
    "ip_hash",
    "power_of_two",
}

# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------


def init_db() -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute(
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
    *,
    algorithm: str,
    server_name: str,
    endpoint: str,
    start_ts: float,
    end_ts: float,
    success: bool,
    client_ip: Optional[str] = None,
) -> None:
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute(
            """
            INSERT INTO requests
                (algorithm, server_name, endpoint,
                 start_time, end_time, total_time,
                 success, client_ip, created_at)
            VALUES (?,?,?,?,?,?,?,?,?)
            """,
            (
                algorithm,
                server_name,
                endpoint,
                start_ts,
                end_ts,
                end_ts - start_ts,
                1 if success else 0,
                client_ip,
                time.time(),
            ),
        )
        conn.commit()
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Backend model
# ---------------------------------------------------------------------------


class BackendServer:
    """One running container instance."""

    def __init__(self, name: str, port: int) -> None:
        self.name = name
        self.port = port
        self.url = f"http://127.0.0.1:{port}"
        self.active_connections: int = 0

    def to_dict(self) -> Dict[str, object]:
        return {
            "name": self.name,
            "url": self.url,
            "active_connections": self.active_connections,
        }


# ---------------------------------------------------------------------------
# Dynamic pool  –  queries Docker socket to find live containers
# ---------------------------------------------------------------------------


class DynamicPool:
    """
    Maintains the list of healthy BackendServer instances by periodically
    querying the Docker daemon via its Unix socket.

    Containers are considered *available* when they appear in `docker ps`
    (i.e. status == running) AND their /health endpoint returns 200.
    """

    def __init__(self) -> None:
        self._servers: List[BackendServer] = []
        self._lock = asyncio.Lock()
        self._docker_client: Optional[httpx.AsyncClient] = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        self._docker_client = httpx.AsyncClient(
            base_url="http://docker",
            transport=httpx.AsyncHTTPTransport(uds="/var/run/docker.sock"),
            timeout=5.0,
        )
        await self._refresh()
        asyncio.create_task(self._refresh_loop())

    async def stop(self) -> None:
        if self._docker_client:
            await self._docker_client.aclose()

    # ------------------------------------------------------------------
    # Internal refresh
    # ------------------------------------------------------------------

    async def _running_container_names(self) -> List[str]:
        """Ask Docker which of our managed containers are currently running."""
        try:
            resp = await self._docker_client.get("/containers/json")
            resp.raise_for_status()
            running: List[str] = []
            for c in resp.json():
                names = [n.lstrip("/") for n in c.get("Names", [])]
                for n in names:
                    if n in MANAGED_CONTAINERS:
                        running.append(n)
            return running
        except Exception as exc:
            print(f"[pool] Docker query failed: {exc}")
            return []

    async def _is_healthy(self, server: BackendServer) -> bool:
        """
        Check that the container's HTTP server is reachable.

        The managed image (tr23malyarchuk/pa-tr23malyarchuk) does not expose
        a /health endpoint, so we simply open a TCP connection to the port.
        Any HTTP response (even 404) means the server is up and accepting
        connections; a connection error means it is not.
        """
        import asyncio as _asyncio
        try:
            # asyncio.open_connection is a pure TCP check – no HTTP needed.
            _reader, _writer = await _asyncio.wait_for(
                _asyncio.open_connection("127.0.0.1", server.port),
                timeout=3.0,
            )
            _writer.close()
            try:
                await _writer.wait_closed()
            except Exception:
                pass
            return True
        except Exception:
            return False

    async def _refresh(self) -> None:
        names = await self._running_container_names()

        # Build candidate list preserving existing objects (keeps active_connections)
        existing = {s.name: s for s in self._servers}
        candidates: List[BackendServer] = []
        for name in names:
            port = MANAGED_CONTAINERS[name]
            srv = existing.get(name) or BackendServer(name, port)
            candidates.append(srv)

        # Health-check all candidates concurrently
        checks = await asyncio.gather(*[self._is_healthy(s) for s in candidates])
        healthy = [s for s, ok in zip(candidates, checks) if ok]

        async with self._lock:
            self._servers = healthy

        names_str = [s.name for s in healthy] or ["(none)"]
        print(f"[pool] Live servers: {', '.join(names_str)}")

    async def _refresh_loop(self) -> None:
        while True:
            await asyncio.sleep(POOL_REFRESH_INTERVAL)
            await self._refresh()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def get_servers(self) -> List[BackendServer]:
        async with self._lock:
            return list(self._servers)

    async def wait_for_any(self, timeout: float = 30.0) -> List[BackendServer]:
        """
        Block until at least one healthy server is available.
        Useful during startup or when scriptA.sh hasn't spun up containers yet.
        """
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            servers = await self.get_servers()
            if servers:
                return servers
            await asyncio.sleep(RETRY_WAIT)
            await self._refresh()
        return []


pool = DynamicPool()


# ---------------------------------------------------------------------------
# Load-balancing algorithms
# ---------------------------------------------------------------------------

_rr_index: int = 0


def _choose_round_robin(servers: List[BackendServer]) -> BackendServer:
    global _rr_index
    srv = servers[_rr_index % len(servers)]
    _rr_index += 1
    return srv


def _choose_random(servers: List[BackendServer]) -> BackendServer:
    return random.choice(servers)


def _choose_least_connections(servers: List[BackendServer]) -> BackendServer:
    return min(servers, key=lambda s: s.active_connections)


def _ip_to_int(ip: str) -> int:
    try:
        return int(ipaddress.ip_address(ip))
    except ValueError:
        return 0


def _basic_hash(v: int) -> int:
    v = (v ^ 0x9E3779B97F4A7C15) & ((1 << 64) - 1)
    v = (v * 0xBF58476D1CE4E5B9) & ((1 << 64) - 1)
    return v & 0xFFFFFFFFFFFFFFFF


def _choose_ip_hash(servers: List[BackendServer], client_ip: str) -> BackendServer:
    h = _basic_hash(_ip_to_int(client_ip))
    return servers[h % len(servers)]


def _choose_power_of_two(servers: List[BackendServer]) -> BackendServer:
    if len(servers) == 1:
        return servers[0]
    i1, i2 = random.sample(range(len(servers)), 2)
    a, b = servers[i1], servers[i2]
    return a if a.active_connections <= b.active_connections else b


def choose_backend(
    algorithm: str,
    servers: List[BackendServer],
    client_ip: Optional[str] = None,
) -> BackendServer:
    if not servers:
        raise RuntimeError("No healthy backend servers available")
    dispatch = {
        "round_robin": lambda: _choose_round_robin(servers),
        "random": lambda: _choose_random(servers),
        "least_connections": lambda: _choose_least_connections(servers),
        "ip_hash": lambda: _choose_ip_hash(servers, client_ip or "127.0.0.1"),
        "power_of_two": lambda: _choose_power_of_two(servers),
    }
    if algorithm not in dispatch:
        raise ValueError(f"Unknown algorithm: {algorithm!r}")
    return dispatch[algorithm]()


# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------

app = FastAPI(title="Reverse Proxy Load Balancer")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)
http_client: httpx.AsyncClient


@app.on_event("startup")
async def on_startup() -> None:
    global http_client
    init_db()
    http_client = httpx.AsyncClient(timeout=300.0)
    await pool.start()


@app.on_event("shutdown")
async def on_shutdown() -> None:
    await http_client.aclose()
    await pool.stop()


# ---------------------------------------------------------------------------
# Status / admin  (terminal-friendly, no frontend needed)
# ---------------------------------------------------------------------------


@app.get("/servers", summary="List currently healthy backends")
async def list_servers() -> Dict[str, object]:
    """
    Returns all containers that are currently running and healthy.
    Useful for monitoring from the terminal:

        watch -n 2 'curl -s http://localhost:8000/servers | python3 -m json.tool'
    """
    servers = await pool.get_servers()
    return {
        "healthy_count": len(servers),
        "servers": [s.to_dict() for s in servers],
        "algorithms": sorted(SUPPORTED_ALGORITHMS),
    }


@app.get("/stats", summary="Per-server request stats from DB")
async def stats() -> Dict[str, object]:
    """
    Quick summary straight from SQLite – no external tools needed.

        curl -s http://localhost:8000/stats | python3 -m json.tool
    """
    conn = sqlite3.connect(DB_PATH)
    try:
        rows = conn.execute(
            """
            SELECT server_name,
                   COUNT(*)                           AS total,
                   SUM(success)                       AS ok,
                   ROUND(AVG(total_time), 3)          AS avg_s,
                   ROUND(MIN(total_time), 3)          AS min_s,
                   ROUND(MAX(total_time), 3)          AS max_s
            FROM requests
            GROUP BY server_name
            ORDER BY server_name
            """
        ).fetchall()
    finally:
        conn.close()

    return {
        "by_server": [
            {
                "server": r[0],
                "total": r[1],
                "success": r[2],
                "avg_s": r[3],
                "min_s": r[4],
                "max_s": r[5],
            }
            for r in rows
        ]
    }


@app.get("/recent", summary="Last N requests from DB")
async def recent(n: int = 50) -> Dict[str, object]:
    """
    Returns the last N requests logged in the database.
    Used by the web admin panel.
    """
    conn = sqlite3.connect(DB_PATH)
    try:
        rows = conn.execute(
            """
            SELECT server_name, algorithm, endpoint,
                   ROUND(total_time, 3), success,
                   datetime(created_at, 'unixepoch', 'localtime') as ts
            FROM requests
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (n,),
        ).fetchall()
    finally:
        conn.close()

    return {
        "requests": [
            {
                "server": r[0],
                "algorithm": r[1],
                "endpoint": r[2],
                "total_time": r[3],
                "success": bool(r[4]),
                "timestamp": r[5],
            }
            for r in rows
        ]
    }


# ---------------------------------------------------------------------------
# Core proxy helper  (retries across healthy servers)
# ---------------------------------------------------------------------------


async def _proxy(
    *,
    file: UploadFile,
    algorithm: str,
    client_ip: Optional[str],
    backend_path: str,
    endpoint_name: str,
    default_content_type: str,
    response_media_type: str,
    response_filename_suffix: str,
    timeout: float,
) -> StreamingResponse | JSONResponse:
    if algorithm not in SUPPORTED_ALGORITHMS:
        raise HTTPException(status_code=400, detail=f"Unknown algorithm: {algorithm!r}")

    file_bytes = await file.read()
    filename = file.filename or "input"

    last_error: str = "No healthy servers"
    tried: List[str] = []

    for attempt in range(MAX_RETRIES):
        # Wait for at least one healthy server (covers the case where scriptA.sh
        # hasn't started the next container yet after a busy threshold is hit).
        servers = await pool.wait_for_any(timeout=30.0)
        if not servers:
            raise HTTPException(
                status_code=503,
                detail="No backend containers are running. Start scriptA.sh first.",
            )

        # Exclude already-tried servers so we don't hammer the same broken one
        candidates = [s for s in servers if s.name not in tried] or servers

        try:
            server = choose_backend(algorithm, candidates, client_ip)
        except Exception as exc:
            raise HTTPException(status_code=500, detail=str(exc))

        tried.append(server.name)
        server.active_connections += 1
        start_ts = time.time()
        success = False
        result_bytes: Optional[bytes] = None

        try:
            resp = await http_client.post(
                f"{server.url}{backend_path}",
                files={
                    "file": (
                        filename,
                        BytesIO(file_bytes),
                        file.content_type or default_content_type,
                    )
                },
                timeout=timeout,
            )
            if resp.status_code == 200:
                result_bytes = resp.content
                success = True
            else:
                last_error = f"{server.name}: HTTP {resp.status_code} – {resp.text[:200]}"
        except httpx.TransportError as exc:
            last_error = f"{server.name}: connection error – {exc}"
            # Container may have just disappeared; trigger an immediate refresh
            asyncio.create_task(pool._refresh())
        except Exception as exc:
            last_error = f"{server.name}: {exc}"
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

        if success and result_bytes is not None:
            stem = Path(filename).stem
            return StreamingResponse(
                BytesIO(result_bytes),
                media_type=response_media_type,
                headers={
                    "X-Chosen-Server": server.name,
                    "X-Algorithm": algorithm,
                    "X-Total-Time": str(round(end_ts - start_ts, 3)),
                    "X-Attempt": str(attempt + 1),
                    "Content-Disposition": (
                        f'attachment; filename="{stem}{response_filename_suffix}"'
                    ),
                },
            )

        # Brief pause before next attempt so the next container has time to start
        if attempt < MAX_RETRIES - 1:
            await asyncio.sleep(RETRY_WAIT)

    return JSONResponse(
        status_code=502,
        content={
            "error": last_error,
            "tried_servers": tried,
            "algorithm": algorithm,
        },
    )


# ---------------------------------------------------------------------------
# Public conversion endpoints
# ---------------------------------------------------------------------------


@app.post("/file-request", summary="WAV → MP3")
async def wav_to_mp3(
    file: UploadFile = File(...),
    algorithm: str = Form("round_robin"),
    client_ip: Optional[str] = Form(None),
):
    return await _proxy(
        file=file,
        algorithm=algorithm,
        client_ip=client_ip,
        backend_path="/convert/wav-to-mp3",
        endpoint_name="/file-request",
        default_content_type="audio/wav",
        response_media_type="audio/mpeg",
        response_filename_suffix=".mp3",
        timeout=60.0,
    )


@app.post("/pdf2png", summary="PDF → PNG (zip)")
async def pdf2png(
    file: UploadFile = File(...),
    algorithm: str = Form("round_robin"),
    client_ip: Optional[str] = Form(None),
):
    return await _proxy(
        file=file,
        algorithm=algorithm,
        client_ip=client_ip,
        backend_path="/convert/pdf-to-png",
        endpoint_name="/pdf2png",
        default_content_type="application/pdf",
        response_media_type="application/zip",
        response_filename_suffix="_pages.zip",
        timeout=120.0,
    )


@app.post("/webp2png", summary="WEBP → PNG")
async def webp2png(
    file: UploadFile = File(...),
    algorithm: str = Form("round_robin"),
    client_ip: Optional[str] = Form(None),
):
    return await _proxy(
        file=file,
        algorithm=algorithm,
        client_ip=client_ip,
        backend_path="/convert/webp-to-png",
        endpoint_name="/webp2png",
        default_content_type="image/webp",
        response_media_type="image/png",
        response_filename_suffix=".png",
        timeout=60.0,
    )


@app.post("/rar2zip", summary="RAR → ZIP")
async def rar2zip(
    file: UploadFile = File(...),
    algorithm: str = Form("round_robin"),
    client_ip: Optional[str] = Form(None),
):
    return await _proxy(
        file=file,
        algorithm=algorithm,
        client_ip=client_ip,
        backend_path="/convert/rar-to-zip",
        endpoint_name="/rar2zip",
        default_content_type="application/vnd.rar",
        response_media_type="application/zip",
        response_filename_suffix=".zip",
        timeout=300.0,
    )


@app.post("/ziprar", summary="RAR → ZIP (alias)")
async def ziprar(
    file: UploadFile = File(...),
    algorithm: str = Form("round_robin"),
    client_ip: Optional[str] = Form(None),
):
    return await _proxy(
        file=file,
        algorithm=algorithm,
        client_ip=client_ip,
        backend_path="/convert/rar-to-zip",
        endpoint_name="/ziprar",
        default_content_type="application/vnd.rar",
        response_media_type="application/zip",
        response_filename_suffix=".zip",
        timeout=300.0,
    )