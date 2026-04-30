# Load balancing system implementation

## Backend-pools

- AUDIO_SERVERS: two converter exemplars
  - audio1: http://127.0.0.1:8081
  - audio2: http://127.0.0.1:8082

## Algorithms

- Round Robin:
  - Implemented in 'gateway' (main.py).
  - For each request to /file-request the next server from the list is being chosen.
  - Result: 'x-chosen-server: audio1|audio2'.

## Converter

- Separate service (FastAPI), containerized in tr23malyarchuk/pa-converter:dev image.
- Endpoints:
  - /health - for health-check.
  - /convert/wav-to-mp3 - audio convertation (ffmpeg).

## Experiment

- Two containers from the one image:
  - Container 1: port 8081
  - Container 2: port 8082
- Validation:
  - 'x-chosen-server' title shows equal balancing.

