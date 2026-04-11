import subprocess
import sys
import time

PROCS = [
    ["uvicorn", "main:app", "--reload", "--port", "8000"],
    ["uvicorn", "converter_wav2mp3:app", "--reload", "--port", "9001"],
    ["uvicorn", "converter_pdf2png:app", "--reload", "--port", "9002"],
    ["uvicorn", "converter_webp2png:app", "--reload", "--port", "9003"],
    ["uvicorn", "converter_rar2zip:app", "--reload", "--port", "9005"],
]

def main():
    processes = []
    try:
        for cmd in PROCS:
            print("Starting:", " ".join(cmd))
            p = subprocess.Popen(cmd)
            processes.append(p)
            time.sleep(0.5)
        print("All backend services started. Press Ctrl+C to stop.")
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

if __name__ == "__main__":
    sys.exit(main())

