from fastapi import FastAPI, UploadFile, File
from fastapi.responses import FileResponse, JSONResponse
import uuid
import os
import subprocess

app = FastAPI(title="WAV to MP3 Converter")

# Папка для временных файлов
TMP_DIR = "tmp_conv"
os.makedirs(TMP_DIR, exist_ok=True)


def convert_wav_to_mp3(input_path: str, output_path: str):
    """
    Конвертация wav -> mp3 через ffmpeg.
    Пример команды:
    ffmpeg -i input.wav -vn -ar 44100 -ac 2 -b:a 192k output.mp3
    """
    cmd = [
        "ffmpeg",
        "-y",
        "-i",
        input_path,
        "-vn",
        "-ar",
        "44100",
        "-ac",
        "2",
        "-b:a",
        "192k",
        output_path,
    ]
    subprocess.check_call(cmd)


@app.post("/convert/wav-to-mp3")
async def wav_to_mp3(file: UploadFile = File(...)):
    # Проверяем расширение
    if not file.filename.lower().endswith(".wav"):
        return JSONResponse(
            status_code=400,
            content={"error": "Only .wav files are supported"},
        )

    # Генерируем временные имена
    in_id = uuid.uuid4().hex
    out_id = uuid.uuid4().hex
    input_path = os.path.join(TMP_DIR, f"{in_id}.wav")
    output_path = os.path.join(TMP_DIR, f"{out_id}.mp3")

    # Сохраняем входной файл
    with open(input_path, "wb") as f:
        f.write(await file.read())

    try:
        # Запускаем конвертацию
        convert_wav_to_mp3(input_path, output_path)
    except subprocess.CalledProcessError as e:
        return JSONResponse(
            status_code=500,
            content={"error": f"ffmpeg failed: {e}"},
        )

    # Можно позже добавить удаление входного файла/очистку
    return FileResponse(
        output_path,
        media_type="audio/mpeg",
        filename=file.filename.rsplit(".", 1)[0] + ".mp3",
    )

# start with:
# uvicorn converter_wav2mp3:app --reload --port 9001

