"""
run_all.py  —  запускає балансировщик + усі сервіси конвертації.

Запускати з будь-якої директорії:
    python3 backend/run_all.py
або з самої папки backend/:
    python3 run_all.py
"""
import subprocess
import sys
import time
from pathlib import Path

# Завжди використовуємо backend/ як робочу директорію,
# незалежно від того, звідки запущений скрипт.
BACKEND_DIR = Path(__file__).resolve().parent

PROCS = [
    ["uvicorn", "main:app",                         "--port", "8000"],
    ["uvicorn", "services.converter_wav2mp3:app",   "--port", "9001"],
    ["uvicorn", "services.converter_pdf2png:app",   "--port", "9002"],
    ["uvicorn", "services.converter_webp2png:app",  "--port", "9003"],
    ["uvicorn", "services.converter_rar2zip:app",   "--port", "9005"],
]


def main() -> int:
    processes = []
    try:
        for cmd in PROCS:
            print("Starting:", " ".join(cmd))
            p = subprocess.Popen(cmd, cwd=BACKEND_DIR)   # <-- ключове виправлення
            processes.append(p)
            time.sleep(0.5)
        print("\nAll backend services started.")
        print("  Balancer  → http://localhost:8000")
        print("  Docs      → http://localhost:8000/docs")
        print("  Pool CLI  → python3 backend/pool_cli.py --help")
        print("\nPress Ctrl+C to stop all services.\n")
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nStopping all services...")
    finally:
        for p in processes:
            if p.poll() is None:
                p.terminate()
        for p in processes:
            try:
                p.wait(timeout=5)
            except subprocess.TimeoutExpired:
                p.kill()
        print("All services stopped.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

