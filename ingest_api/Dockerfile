# Imagen del API de ingesta. Contexto de build: ingest_api/
FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    STORAGE_PATH=/data/readings.jsonl

WORKDIR /srv
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY app ./app

# Usuario sin privilegios; /data es el único directorio donde escribe.
RUN useradd --uid 10001 --no-create-home skilliket \
    && mkdir -p /data && chown skilliket /data
USER skilliket

EXPOSE 8000
# start-period amplio: en equipos con CPU limitada el import de la app tarda >15 s.
HEALTHCHECK --interval=5s --timeout=10s --start-period=90s --retries=6 \
    CMD ["python", "-c", "import sys, urllib.request; sys.exit(urllib.request.urlopen('http://127.0.0.1:8000/health', timeout=2).status != 200)"]

# Un solo worker: el índice de duplicados y el lock de JsonlStorage viven en memoria (ADR 0001).
CMD ["uvicorn", "app.main:create_app", "--factory", "--host", "0.0.0.0", "--port", "8000"]
