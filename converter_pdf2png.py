import os
import uuid
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import StreamingResponse

from pdf2image import convert_from_path

TMP_DIR = Path("tmp_conv")
TMP_DIR.mkdir(exist_ok=True)

app = FastAPI(title="PDF to PNG converter")

POPPLER_PATH: Optional[str] = None  # если нужен poppler_path под Windows — тут можно указать путь


def save_upload_to_tmp(file: UploadFile, suffix: str) -> Path:
    ext = suffix if suffix.startswith(".") else f".{suffix}"
    tmp_name = f"{uuid.uuid4().hex}{ext}"
    tmp_path = TMP_DIR / tmp_name
    content = file.file.read()
    with open(tmp_path, "wb") as f:
        f.write(content)
    return tmp_path


@app.post("/convert/pdf-to-png")
async def convert_pdf_to_png(file: UploadFile = File(...)):
    """
    Принимает PDF и возвращает ZIP-архив с PNG-страницами.
    """
    if not file.filename.lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="Expected a PDF file")

    try:
        tmp_pdf = save_upload_to_tmp(file, ".pdf")

        # Конвертация PDF в изображения
        images = convert_from_path(
            str(tmp_pdf),
            dpi=200,
            poppler_path=POPPLER_PATH,
        )
        if not images:
            raise HTTPException(status_code=500, detail="No pages found in PDF")

        # Сохраняем все страницы в PNG во временный архив
        import zipfile
        zip_name = f"{uuid.uuid4().hex}.zip"
        zip_path = TMP_DIR / zip_name

        with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            for idx, img in enumerate(images, start=1):
                png_name = f"page_{idx}.png"
                tmp_png_path = TMP_DIR / f"{uuid.uuid4().hex}.png"
                img.save(tmp_png_path, "PNG")
                zf.write(tmp_png_path, arcname=png_name)
                os.remove(tmp_png_path)

        def iterfile():
            with open(zip_path, "rb") as f:
                yield from f
            # опционально: очищать zip после отдачи
            try:
                os.remove(zip_path)
                os.remove(tmp_pdf)
            except OSError:
                pass

        headers = {
            "Content-Disposition": 'attachment; filename="pdf_pages.zip"',
        }

        return StreamingResponse(
            iterfile(),
            media_type="application/zip",
            headers=headers,
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Conversion error: {e}")
    finally:
        try:
            file.file.close()
        except Exception:
            pass

