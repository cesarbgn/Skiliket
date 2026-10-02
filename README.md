# Skilliket — Estación meteorológica simulada

Prototipo del TEC de Monterrey GDA. Una estación meteorológica emulada envía lecturas a un API REST que las valida y las guarda. Esta entrega corre solo en local; el objetivo en Azure se describe en `docs/adr/` y en el diagrama C4 (fase posterior).

## Estado

- **Fase A — cerrada y verificada:** contrato v1 (`schema/`) y API de ingesta (`ingest_api/`), 98 pruebas en verde y verificación contra uvicorn real. El registro del trabajo (propuesta, reportes y volcados de revisión) está en [`docs/historial/`](docs/historial/).
- **Pendiente:** generador de la estación, Docker/compose y diagramas C4.

## Estructura

| Ruta | Contenido |
|---|---|
| `ingest_api/app/` | API FastAPI: modelo, autenticación, errores, almacenamiento |
| `ingest_api/tests/` | Pruebas (pytest) |
| `schema/weather_reading.v1.schema.json` | JSON Schema exportado del modelo Pydantic |
| `scripts/gen_station_key.py` | Genera una clave de estación y su hash SHA-256 |
| `scripts/export_schema.py` | Regenera el JSON Schema |
| `docs/adr/` | Decisiones de arquitectura |

## Ejecutar en local

Requiere Python 3.13.

```powershell
py -3.13 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r ingest_api\requirements-dev.txt

# Una clave por línea de salida: guarda STATION_KEY para la estación y usa SHA256 en el API.
.\.venv\Scripts\python.exe scripts\gen_station_key.py

$env:STATION_KEY_HASHES = '{"GDL-001": ["<sha256>"]}'
cd ingest_api
..\.venv\Scripts\python.exe -m uvicorn app.main:create_app --factory --port 8000
```

- Documentación interactiva: http://127.0.0.1:8000/docs
- Variables de entorno: ver `.env.example`.

## Pruebas

```powershell
cd ingest_api
..\.venv\Scripts\python.exe -m pytest
```

Si cambias `app/models.py`, regenera el esquema con `python scripts/export_schema.py`. Una prueba falla si el archivo exportado no coincide con el modelo.

## API

`POST /v1/readings` con `Content-Type: application/json` y la cabecera `X-Station-Key`.

| Código | Caso |
|---|---|
| 201 | Lectura válida guardada |
| 400 | JSON mal formado (`line`, `column`) |
| 401 | Falta la clave o no es válida |
| 403 | El `station_id` del cuerpo no corresponde a la clave |
| 409 | Duplicado (`station_id` + `timestamp`): al reintentar, tratar como éxito |
| 413 | Cuerpo mayor a 4 KB |
| 415 | Content-Type distinto de `application/json` |
| 422 | No cumple el esquema (`errors: [{field, code, message, value}]`) |

Los errores usan `application/problem+json` (RFC 9457). `GET /health` no requiere autenticación.
