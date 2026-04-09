from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
import random
import ipaddress
import asyncio
from typing import List, Dict, Callable

# --- Простая модель backend-серверов в памяти ---

class BackendServer:
    def __init__(self, name: str, url: str):
        self.name = name
        self.url = url
        self.active_connections = 0

    def to_dict(self):
        return {
            "name": self.name,
            "url": self.url,
            "active_connections": self.active_connections,
        }


# Предопределённый пул серверов (позже можно вынести в БД / конфиг)
servers: List[BackendServer] = [
    BackendServer("srv1", "http://srv1:8001"),
    BackendServer("srv2", "http://srv2:8002"),
    BackendServer("srv3", "http://srv3:8003"),
]

app = FastAPI(title="Simple Load Balancer Prototype")

# --- Алгоритмы балансировки ---

_rr_index = 0


def choose_round_robin() -> BackendServer:
    """
    Классический Round Robin:
    просто крутимся по списку servers по кругу.
    """
    global _rr_index
    if not servers:
        raise RuntimeError("No backend servers configured")
    server = servers[_rr_index % len(servers)]
    _rr_index += 1
    return server


def choose_random() -> BackendServer:
    """
    Случайный выбор сервера.
    """
    if not servers:
        raise RuntimeError("No backend servers configured")
    return random.choice(servers)


def choose_least_connections() -> BackendServer:
    """
    Выбор сервера с наименьшим количеством активных соединений.
    """
    if not servers:
        raise RuntimeError("No backend servers configured")
    return min(servers, key=lambda s: s.active_connections)


def ip_to_int(ip_str: str) -> int:
    """
    Конвертация IPv4/IPv6 в целое число (аналог Convert_IP_to_Integer).
    """
    return int(ipaddress.ip_address(ip_str))


def basic_hash(value: int) -> int:
    """
    Простейший hash (Basic_Hash_Function) — можно усложнить позже.
    """
    value = (value ^ 0x9e3779b97f4a7c15) & ((1 << 64) - 1)
    value = (value * 0xbf58476d1ce4e5b9) & ((1 << 64) - 1)
    return value & 0xFFFFFFFFFFFFFFFF


def choose_ip_hash(client_ip: str) -> BackendServer:
    """
    IP Hash по схеме из методички: IP -> целое -> hash -> modulo.
    """
    if not servers:
        raise RuntimeError("No backend servers configured")
    numeric_ip = ip_to_int(client_ip)
    hash_value = basic_hash(numeric_ip)
    idx = hash_value % len(servers)
    return servers[idx]


def choose_power_of_two() -> BackendServer:
    """
    Power of Two Random Choices:
    выбираем 2 случайных сервера и берём тот, у кого меньше active_connections.
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


@app.post("/request")
async def handle_request(request: Request):
    """
    Эндпоинт, который:
    - выбирает сервер по алгоритму;
    - увеличивает active_connections;
    - имитирует обработку (asyncio.sleep);
    - уменьшает active_connections.
    """
    body: Dict = await request.json()
    algo_name = body.get("algorithm", "round_robin")
    client_ip = body.get("client_ip")  # для ip_hash
    # искусственная длительность обработки (в секундах),
    # можно передавать из клиента или оставить фиксированной
    processing_time = float(body.get("processing_time", 0.2))

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

    # инкремент active_connections
    server.active_connections += 1

    try:
        # имитация обработки запроса на backend-сервере
        await asyncio.sleep(processing_time)
    finally:
        # декремент active_connections даже при ошибках
        server.active_connections -= 1

    return {
        "chosen_server": server.to_dict(),
        "algorithm": algo_name,
        "client_ip": client_ip,
        "processing_time": processing_time,
    }

# Запуск:
# uvicorn main:app --reload --port 8000

