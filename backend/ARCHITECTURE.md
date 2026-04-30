# 2026-04-30: converter decision
- Use one converter service image with several endpoints (/convert/*).
- Pools in main.py:
  - AUDIO_SERVERS - several 'converter' instances, use /convert/wav-to-mp3.
  - PDF_SERVERS - same instances or the separate pool, but the single image.
- scriptA.sh will manage 'converter' (srv1...srvN) instances count and their resources (CPU, ports, etc).

