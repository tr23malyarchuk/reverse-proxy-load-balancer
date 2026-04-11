from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import StreamingResponse

from io import BytesIO
from pathlib import Path

from PIL import Image

app = FastAPI(title="WEBP to PNG converter")


def convert_webp_bytes_to_png_bytes(data: bytes) -> bytes:
    try:
        with Image.open(BytesIO(data)) as im:
            if im.mode not in ("RGB", "RGBA"):
                im = im.convert("RGBA")
            buf = BytesIO()
            im.save(buf, format="PNG")
            buf.seek(0)
            return buf.read()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Conversion error: {e}")


@app.post("/convert/webp-to-png")
async def webp_to_png(file: UploadFile = File(...)):
    filename = file.filename or "input.webp"
    if not filename.lower().endswith(".webp"):
        raise HTTPException(status_code=400, detail="Expected a WEBP file")

    try:
        data = await file.read()
        if not data:
            raise HTTPException(status_code=400, detail="Empty file")

        png_bytes = convert_webp_bytes_to_png_bytes(data)

        out_name = f"{Path(filename).stem}.png"
        headers = {
            "Content-Disposition": f'attachment; filename="{out_name}"',
        }

        return StreamingResponse(
            BytesIO(png_bytes),
            media_type="image/png",
            headers=headers,
        )
    finally:
        await file.close()

    # uvicorn converter_webp2png:app --reload --port 9003

