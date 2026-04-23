from fastapi import FastAPI, UploadFile, File
from fastapi.responses import FileResponse, JSONResponse
import uuid
import os
import subprocess

app = FastAPI(title="WAV to MP3 Converter")

TMP_DIR = "tmp_conv"
os.makedirs(TMP_DIR, exist_ok=True)


def convert_wav_to_mp3(input_path: str, output_path: str):
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
    if not file.filename.lower().endswith(".wav"):
        return JSONResponse(
            status_code=400,
            content={"error": "Only .wav files are supported"},
        )

    in_id = uuid.uuid4().hex
    out_id = uuid.uuid4().hex
    input_path = os.path.join(TMP_DIR, f"{in_id}.wav")
    output_path = os.path.join(TMP_DIR, f"{out_id}.mp3")

    with open(input_path, "wb") as f:
        f.write(await file.read())

    try:
        convert_wav_to_mp3(input_path, output_path)
    except subprocess.CalledProcessError as e:
        return JSONResponse(
            status_code=500,
            content={"error": f"ffmpeg failed: {e}"},
        )

    return FileResponse(
        output_path,
        media_type="audio/mpeg",
        filename=file.filename.rsplit(".", 1)[0] + ".mp3",
    )

# uvicorn converter_wav2mp3:app --reload --port 9001

