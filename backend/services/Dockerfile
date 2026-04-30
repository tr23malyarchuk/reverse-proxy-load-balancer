FROM python:3.11-slim
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1
WORKDIR /app/services
RUN apt-get update && \
    apt-get install -y --no-install-recommends ffmpeg poppler-utils unrar-free && \
    rm -rf /var/lib/apt/lists/*
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
ARG APP_MODULE
ARG APP_PORT
ENV APP_MODULE=${APP_MODULE}
ENV APP_PORT=${APP_PORT}
CMD ["sh", "-c", "uvicorn ${APP_MODULE} --host 0.0.0.0 --port ${APP_PORT}"]

