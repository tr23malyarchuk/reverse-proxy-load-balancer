import sqlite3
import time
from pathlib import Path

import httpx

BALANCER_URL = "http://localhost:8000"
ROOT_DIR = Path(__file__).parent.parent
DB_PATH = ROOT_DIR / "data" / "requests.db"


def fetch_servers() -> dict:
    try:
        r = httpx.get(f"{BALANCER_URL}/servers", timeout=2.0)
        return r.json()
    except Exception as e:
        return {"error": str(e)}


def fetch_stats() -> list:
    if not DB_PATH.exists():
        return []
    conn = sqlite3.connect(DB_PATH)
    try:
        rows = conn.execute(
            """
            SELECT
                server_name,
                COUNT(*)                  AS total,
                SUM(success)              AS ok,
                ROUND(AVG(total_time), 2) AS avg_s,
                ROUND(MIN(total_time), 2) AS min_s,
                ROUND(MAX(total_time), 2) AS max_s,
                algorithm
            FROM requests
            GROUP BY server_name, algorithm
            ORDER BY server_name, algorithm
            """
        ).fetchall()
    finally:
        conn.close()
    return rows


def fetch_recent(n: int = 8) -> list:
    if not DB_PATH.exists():
        return []
    conn = sqlite3.connect(DB_PATH)
    try:
        rows = conn.execute(
            """
            SELECT server_name, algorithm, endpoint,
                   ROUND(total_time, 2), success,
                   datetime(created_at, 'unixepoch', 'localtime')
            FROM requests
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (n,),
        ).fetchall()
    finally:
        conn.close()
    return rows


def sep(char="-", width=62):
    print(char * width)


def main() -> None:
    now = time.strftime("%H:%M:%S")
    sep("=")
    print(f"  LOAD BALANCER ADMIN                          {now}")
    sep("=")

    # -- Живі сервери -------------------------------------------------------
    print("\n  [ЖИВI СЕРВЕРИ]")
    sep()
    data = fetch_servers()
    if "error" in data:
        print(f"  ! Балансировщик недоступний: {data['error']}")
        print("  ! Запустіть: uvicorn main:app --host 0.0.0.0 --port 8000")
    else:
        count = data.get("healthy_count", 0)
        servers = data.get("servers", [])
        if count == 0:
            print("  ! Немає живих серверів. Запустіть scriptA.sh")
        else:
            print(f"  Живих серверів: {count}")
            print(f"  {'Iм`я':<8}  {'URL':<26}  {'Активних з`єднань':>18}")
            sep()
            for s in servers:
                print(f"  {s['name']:<8}  {s['url']:<26}  {s['active_connections']:>18}")
        algos = ", ".join(data.get("algorithms", []))
        print(f"\n  Алгоритми: {algos}")

    # -- Статистика по серверах ---------------------------------------------
    print("\n  [СТАТИСТИКА ПО СЕРВЕРАХ]")
    sep()
    stats = fetch_stats()
    if not stats:
        print("  Запитів ще не було.")
    else:
        print(f"  {'Сервер':<8}  {'Алгоритм':<18}  {'Всього':>7}  {'OK':>6}  {'Сер.с':>6}  {'Мін.с':>6}  {'Макс.с':>7}")
        sep()
        for row in stats:
            srv, total, ok, avg_s, min_s, max_s, algo = row
            print(f"  {srv:<8}  {algo:<18}  {total:>7}  {ok:>6}  {avg_s:>6}  {min_s:>6}  {max_s:>7}")

    # -- Останні запити -----------------------------------------------------
    print("\n  [ОСТАННI 8 ЗАПИТIВ]")
    sep()
    recent = fetch_recent()
    if not recent:
        print("  Запитів ще не було.")
    else:
        print(f"  {'Сервер':<8}  {'Алгоритм':<16}  {'Ендпоінт':<14}  {'Час':>5}  {'Статус':<6}  {'Час запиту'}")
        sep()
        for row in recent:
            srv, algo, ep, t, success, ts = row
            status = "OK" if success else "FAIL"
            print(f"  {srv:<8}  {algo:<16}  {ep:<14}  {t:>5}  {status:<6}  {ts}")

    sep("=")
    print("  watch -n 2 python3 admin.py   |   Ctrl+C щоб зупинити")
    sep("=")


if __name__ == "__main__":
    main()

