import sqlite3
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).parent.parent
DB_PATH = ROOT_DIR / "data" / "requests.db"


DDL = """
CREATE TABLE IF NOT EXISTS Machines (
    idMachine   INTEGER PRIMARY KEY AUTOINCREMENT,
    hostname    TEXT    NOT NULL,
    ip_address  TEXT    NOT NULL,
    ssh_port    INTEGER NOT NULL DEFAULT 22,
    description TEXT
);

CREATE TABLE IF NOT EXISTS Services (
    idService     INTEGER PRIMARY KEY AUTOINCREMENT,
    name          TEXT    NOT NULL UNIQUE,
    description   TEXT,
    base_path     TEXT    NOT NULL,
    cpu_intensity TEXT    NOT NULL DEFAULT 'medium'
                  CHECK(cpu_intensity IN ('low','medium','high')),
    docker_image  TEXT    DEFAULT 'tr23malyarchuk/pa-tr23malyarchuk:latest',
    update_period TEXT    DEFAULT 'None'
);

CREATE TABLE IF NOT EXISTS Pools (
    idPool      INTEGER PRIMARY KEY AUTOINCREMENT,
    name        TEXT    NOT NULL UNIQUE,
    service_id  INTEGER NOT NULL,
    algorithm   TEXT    NOT NULL DEFAULT 'round_robin',
    description TEXT,
    FOREIGN KEY (service_id) REFERENCES Services(idService)
);

CREATE TABLE IF NOT EXISTS ServiceInstances (
    idInstance   INTEGER PRIMARY KEY AUTOINCREMENT,
    service_id   INTEGER NOT NULL,
    machine_id   INTEGER NOT NULL,
    container_id TEXT,
    port         INTEGER NOT NULL,
    status       TEXT    NOT NULL DEFAULT 'running'
                 CHECK(status IN ('running','stopped','error')),
    created_at   REAL    NOT NULL,
    updated_at   REAL    NOT NULL,
    FOREIGN KEY (service_id) REFERENCES Services(idService),
    FOREIGN KEY (machine_id) REFERENCES Machines(idMachine)
);

CREATE TABLE IF NOT EXISTS PoolMembers (
    idPoolMember INTEGER PRIMARY KEY AUTOINCREMENT,
    pool_id      INTEGER NOT NULL,
    instance_id  INTEGER NOT NULL,
    weight       INTEGER NOT NULL DEFAULT 1,
    FOREIGN KEY (pool_id)     REFERENCES Pools(idPool),
    FOREIGN KEY (instance_id) REFERENCES ServiceInstances(idInstance)
);

CREATE TABLE IF NOT EXISTS AutoscalingRules (
    idRule           INTEGER PRIMARY KEY AUTOINCREMENT,
    pool_id          INTEGER NOT NULL,
    metric_type      TEXT    NOT NULL DEFAULT 'cpu_percent'
                     CHECK(metric_type IN ('cpu_percent','active_connections','latency_ms')),
    threshold        REAL    NOT NULL,
    action           TEXT    NOT NULL
                     CHECK(action IN ('scale_out','scale_in')),
    cooldown_seconds INTEGER NOT NULL DEFAULT 60,
    min_instances    INTEGER NOT NULL DEFAULT 1,
    max_instances    INTEGER NOT NULL DEFAULT 4,
    FOREIGN KEY (pool_id) REFERENCES Pools(idPool)
);

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
);

CREATE TABLE IF NOT EXISTS Requests (
    idRequest           INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp           REAL    NOT NULL,
    client_ip           TEXT,
    service_id          INTEGER,
    pool_id             INTEGER,
    instance_id         INTEGER,
    algorithm           TEXT    NOT NULL,
    status_code         INTEGER NOT NULL DEFAULT 200,
    latency_ms          REAL    NOT NULL,
    request_size_bytes  INTEGER,
    response_size_bytes INTEGER,
    FOREIGN KEY (service_id)  REFERENCES Services(idService),
    FOREIGN KEY (pool_id)     REFERENCES Pools(idPool),
    FOREIGN KEY (instance_id) REFERENCES ServiceInstances(idInstance)
);

CREATE VIEW IF NOT EXISTS v_requests_full AS
SELECT
    r.idRequest,
    datetime(r.timestamp, 'unixepoch', 'localtime') AS time,
    r.client_ip,
    s.name           AS service,
    p.name           AS pool,
    si.port          AS instance_port,
    r.algorithm,
    r.status_code,
    r.latency_ms,
    r.request_size_bytes,
    r.response_size_bytes
FROM Requests r
LEFT JOIN Services         s  ON s.idService   = r.service_id
LEFT JOIN Pools            p  ON p.idPool      = r.pool_id
LEFT JOIN ServiceInstances si ON si.idInstance = r.instance_id;
"""


SEED_DATA = [
    (
        "INSERT OR IGNORE INTO Machines (idMachine, hostname, ip_address, ssh_port, description) VALUES (?,?,?,?,?)",
        [(1, "localhost", "127.0.0.1", 22, "Local development machine")],
    ),
    (
        "INSERT OR IGNORE INTO Services (idService, name, description, base_path, cpu_intensity, docker_image, update_period) VALUES (?,?,?,?,?,?,?)",
        [
            (1, "wav2mp3",  "Конвертація WAV у MP3",   "/wav2mp3", "medium", "tr23malyarchuk/pa-tr23malyarchuk:latest", "None"),
            (2, "pdf2png",  "Конвертація PDF у PNG",   "/pdf2png", "high",   "tr23malyarchuk/pa-tr23malyarchuk:latest", "None"),
            (3, "webp2png", "Конвертація WEBP у PNG",  "/webp2png", "low",    "tr23malyarchuk/pa-tr23malyarchuk:latest", "None"),
            (4, "rar2zip",  "Перепакування RAR у ZIP", "/ziprar",  "medium", "tr23malyarchuk/pa-tr23malyarchuk:latest", "None"),
        ],
    ),
    (
        "INSERT OR IGNORE INTO Pools (idPool, name, service_id, algorithm, description) VALUES (?,?,?,?,?)",
        [
            (1, "wav2mp3_pool",  1, "round_robin",       "Пул для WAV->MP3 конвертації"),
            (2, "pdf2png_pool",  2, "least_connections", "Пул для PDF->PNG конвертації"),
            (3, "webp2png_pool", 3, "round_robin",       "Пул для WEBP->PNG конвертації"),
            (4, "rar2zip_pool",  4, "round_robin",       "Пул для RAR->ZIP конвертації"),
        ],
    ),
    (
        "INSERT OR IGNORE INTO ServiceInstances (idInstance, service_id, machine_id, container_id, port, status, created_at, updated_at) VALUES (?,?,?,?,?,?,strftime('%s','now'),strftime('%s','now'))",
        [
            (1, 1, 1, "srv1", 8081, "running"),
            (2, 2, 1, "srv2", 8082, "stopped"),
            (3, 3, 1, "srv3", 8083, "stopped"),
            (4, 4, 1, "srv4", 8084, "stopped"),
        ],
    ),
    (
        "INSERT OR IGNORE INTO PoolMembers (pool_id, instance_id, weight) VALUES (?,?,?)",
        [(1, 1, 1), (2, 2, 1), (3, 3, 1), (4, 4, 1)],
    ),
    (
        "INSERT OR IGNORE INTO AutoscalingRules (pool_id, metric_type, threshold, action, cooldown_seconds, min_instances, max_instances) VALUES (?,?,?,?,?,?,?)",
        [
            (1, "cpu_percent", 80.0, "scale_out", 60,  1, 4),
            (1, "cpu_percent",  5.0, "scale_in",  120, 1, 4),
            (2, "cpu_percent", 80.0, "scale_out", 60,  1, 4),
            (2, "cpu_percent",  5.0, "scale_in",  120, 1, 4),
            (3, "cpu_percent", 80.0, "scale_out", 60,  1, 4),
            (3, "cpu_percent",  5.0, "scale_in",  120, 1, 4),
            (4, "cpu_percent", 80.0, "scale_out", 60,  1, 4),
            (4, "cpu_percent",  5.0, "scale_in",  120, 1, 4),
        ],
    ),
]


def create_schema(conn: sqlite3.Connection) -> None:
    # executescript не підтримує параметри — використовуємо для DDL
    for statement in DDL.strip().split(";"):
        s = statement.strip()
        if s:
            conn.execute(s)
    conn.commit()
    print("[db] Таблиці створено.")


def seed(conn: sqlite3.Connection) -> None:
    for sql, rows in SEED_DATA:
        conn.executemany(sql, rows)
    conn.commit()
    print("[db] Базові дані додано.")


def reset(conn: sqlite3.Connection) -> None:
    for view in ["v_requests_full"]:
        conn.execute(f"DROP VIEW IF EXISTS {view}")
    for table in ["AutoscalingRules", "PoolMembers", "ServiceInstances",
                  "Pools", "Services", "Machines", "Requests"]:
        conn.execute(f"DROP TABLE IF EXISTS {table}")
    conn.commit()
    print("[db] Таблиці скинуто.")


def print_summary(conn: sqlite3.Connection) -> None:
    tables = ["Machines", "Services", "Pools",
              "ServiceInstances", "PoolMembers", "AutoscalingRules", "Requests"]
    print("\n[db] Поточний стан таблиць:")
    print(f"  {'Таблиця':<22} {'Рядків':>7}")
    print(f"  {'-'*22} {'-'*7}")
    for t in tables:
        try:
            count = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
            print(f"  {t:<22} {count:>7}")
        except Exception:
            print(f"  {t:<22} {'—':>7}")
    print()


def main() -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    do_reset = "--reset" in sys.argv

    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON")

    try:
        if do_reset:
            confirm = input("УВАГА: --reset видалить всі дані. Продовжити? (yes/no): ")
            if confirm.strip().lower() != "yes":
                print("Скасовано.")
                return
            reset(conn)

        create_schema(conn)
        seed(conn)
        print_summary(conn)
        print("[db] Готово. БД знаходиться в:", DB_PATH)

    finally:
        conn.close()


if __name__ == "__main__":
    main()

