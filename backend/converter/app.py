from fastapi import FastAPI

app = FastAPI(title="Converter placeholder")

@app.get("/health")
async def health():
    return {"status": "ok"}

