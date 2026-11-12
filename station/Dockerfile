# Imagen de la estación simulada (una estación por contenedor). Contexto de build: station/
FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /srv
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY station ./station

RUN useradd --uid 10001 --no-create-home skilliket
USER skilliket

# SIGTERM (docker stop) usa el mismo manejador que SIGINT: vacía el búfer e imprime el resumen.
CMD ["python", "-m", "station.main"]
