import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).parent / "requests.db"

def reset_db():
    if not DB_PATH.exists():
        print(f"{DB_PATH} not found, nothing to reset")
        return

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    try:
        cur.execute("DELETE FROM requests")
        conn.commit()
        print("Table 'requests' cleared")
    except Exception as e:
        print(f"Error while clearing DB: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    reset_db()

