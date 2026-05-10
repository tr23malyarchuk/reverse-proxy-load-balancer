FROM python:3.11-slim

WORKDIR /app

# system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    libmagic1 \
    ffmpeg \
    && rm -rf /var/lib/apt/lists/*

# dependencies
COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# all backend code
COPY . .

# recreate database
RUN python db_schema.py

# create folder for db
RUN mkdir -p /app/data

EXPOSE 8000

# start load balancer
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
