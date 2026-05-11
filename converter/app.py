from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import StreamingResponse
from io import BytesIO
import subprocess
from pdf2image import convert_from_bytes
import zipfile
import tempfile
from pathlib import Path
from PIL import Image

app = FastAPI(title="File Converter Service")

@app.get("/health")
async def health():
    return {"status": "ok"}

@app.post("/convert/wav-to-mp3")
async def wav_to_mp3(file: UploadFile = File(...)):
    data = await file.read()
    with tempfile.NamedTemporaryFile(suffix=".wav", delete=False) as in_f:
        in_f.write(data)
        in_path = in_f.name
    out_path = in_path.replace(".wav", ".mp3")

    try:
        subprocess.check_call(["ffmpeg", "-y", "-i", in_path, out_path])
        with open(out_path, "rb") as f:
            out_bytes = f.read()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"ffmpeg error: {e}")
    finally:
        for p in [in_path, out_path]:
            try:
                Path(p).unlink(missing_ok=True)
            except Exception:
                pass

    return StreamingResponse(
        BytesIO(out_bytes),
        media_type="audio/mpeg",
        headers={"Content-Disposition": 'attachment; filename="output.mp3"'},
    )


@app.post("/convert/pdf-to-png")
async def pdf_to_png(file: UploadFile = File(...)):
    data = await file.read()
    images = convert_from_bytes(data, dpi=150)
    mem = BytesIO()
    with zipfile.ZipFile(mem, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        for idx, img in enumerate(images, start=1):
            buf = BytesIO()
            img.save(buf, format="PNG")
            zf.writestr(f"page_{idx}.png", buf.getvalue())
    mem.seek(0)
    return StreamingResponse(
        mem,
        media_type="application/zip",
        headers={"Content-Disposition": 'attachment; filename="pages.zip"'},
    )


@app.post("/convert/webp-to-png")
async def webp_to_png(file: UploadFile = File(...)):
    data = await file.read()
    try:
        with Image.open(BytesIO(data)) as im:
            if im.mode not in ("RGB", "RGBA"):
                im = im.convert("RGBA")
            buf = BytesIO()
            im.save(buf, format="PNG")
            buf.seek(0)
            return StreamingResponse(
                buf,
                media_type="image/png",
                headers={"Content-Disposition": 'attachment; filename="output.png"'},
            )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Conversion error: {e}")


@app.post("/convert/rar-to-zip")
async def rar_to_zip(file: UploadFile = File(...)):
    data = await file.read()
    import rarfile, os, shutil, uuid
    tmp_id = uuid.uuid4().hex
    tmp_dir = Path(f"/tmp/rar_{tmp_id}")
    tmp_dir.mkdir()
    rar_path = tmp_dir / "input.rar"
    try:
        rar_path.write_bytes(data)
        with rarfile.RarFile(rar_path) as rf:
            rf.extractall(tmp_dir / "out")
        mem = BytesIO()
        with zipfile.ZipFile(mem, "w", zipfile.ZIP_DEFLATED) as zf:
            for f in (tmp_dir / "out").rglob("*"):
                if f.is_file():
                    zf.write(f, f.relative_to(tmp_dir / "out"))
        mem.seek(0)
        return StreamingResponse(
            mem,
            media_type="application/zip",
            headers={"Content-Disposition": 'attachment; filename="output.zip"'},
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"RAR error: {e}")
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)
