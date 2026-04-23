import os
import shutil
import uuid
from pathlib import Path

from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import StreamingResponse

from io import BytesIO
import zipfile
import rarfile

TMP_DIR = Path("tmp_rar2zip")
TMP_DIR.mkdir(exist_ok=True)

app = FastAPI(title="RAR to ZIP converter")


def rar_to_zip_bytes(data: bytes) -> bytes:
    tmp_id = uuid.uuid4().hex
    rar_path = TMP_DIR / f"src_{tmp_id}.rar"
    unpack_dir = TMP_DIR / f"unpack_{tmp_id}"
    zip_path = TMP_DIR / f"out_{tmp_id}.zip"

    unpack_dir.mkdir(parents=True, exist_ok=True)

    try:
        with open(rar_path, "wb") as f:
            f.write(data)

        with rarfile.RarFile(rar_path) as rf:
            rf.extractall(path=str(unpack_dir))

        with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            for root, _, files in os.walk(unpack_dir):
                for fname in files:
                    full_path = Path(root) / fname
                    arcname = full_path.relative_to(unpack_dir)
                    zf.write(str(full_path), arcname=str(arcname))

        with open(zip_path, "rb") as f:
            out_bytes = f.read()

        return out_bytes
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"RAR to ZIP error: {e}")
    finally:
        try:
            if rar_path.exists():
                rar_path.unlink()
        except Exception:
            pass
        try:
            shutil.rmtree(unpack_dir, ignore_errors=True)
        except Exception:
            pass
        try:
            if zip_path.exists():
                zip_path.unlink()
        except Exception:
            pass


@app.post("/convert/rar-to-zip")
async def rar_to_zip(file: UploadFile = File(...)):
    filename = file.filename or "input.rar"
    if not filename.lower().endswith(".rar"):
        raise HTTPException(status_code=400, detail="Expected a RAR file")

    try:
        data = await file.read()
        if not data:
            raise HTTPException(status_code=400, detail="Empty file")

        zip_bytes = rar_to_zip_bytes(data)

        out_name = f"{Path(filename).stem}.zip"
        headers = {
            "Content-Disposition": f'attachment; filename="{out_name}"',
        }

        return StreamingResponse(
            BytesIO(zip_bytes),
            media_type="application/zip",
            headers=headers,
        )
    finally:
        await file.close()

    # uvicorn converter_rar2zip:app --reload --port 9005

