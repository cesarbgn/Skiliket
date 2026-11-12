# Skilliket — Estación meteorológica simulada

Prototipo del TEC de Monterrey GDA. Una estación meteorológica emulada envía lecturas a un API REST que las valida y las guarda. Esta entrega corre solo en local; el objetivo en Azure se describe en `docs/adr/` y en el diagrama C4 (fase posterior).

## Estado

- **Fase A — cerrada y verificada:** contrato v1 (`schema/`) y API de ingesta (`ingest_api/`), 98 pruebas en verde y verificación contra uvicorn real. El registro del trabajo (propuesta, reportes y volcados de revisión) está en [`docs/historial/`](docs/historial/).
- **Fase B — terminada:** estación simulada (`station/`). Incluye el modelo meteorológico de GDL, el emisor con reintentos y búfer, el modo réplica y la inyección de fallas. Pruebas en verde en `station/`, `ingest_api/` y `contract_tests/`, y verificación con uvicorn y 3 procesos de estación reales. Decisiones en [`docs/adr/0002-estacion-simulada.md`](docs/adr/0002-estacion-simulada.md).
- **Fase C — en curso:** imágenes Docker (`ingest_api/Dockerfile`, `station/Dockerfile`), `docker-compose.yml` con 3 estaciones y prueba de estrés (`scripts/stress_docker.py`).
- **Pendiente:** diagramas C4 (Fase D).

## Estructura

| Ruta | Contenido |
|---|---|
| `ingest_api/app/` | API FastAPI: modelo, autenticación, errores, almacenamiento |
| `ingest_api/tests/` | Pruebas del API (pytest) |
| `station/station/` | Estación simulada: configuración, modelo meteorológico, fallas, emisor |
| `station/tests/` | Pruebas de la estación (pytest) |
| `contract_tests/` | Contrato: salida del generador contra el modelo real del API |
| `schema/weather_reading.v1.schema.json` | JSON Schema exportado del modelo Pydantic |
| `scripts/gen_station_key.py` | Genera una clave de estación y su hash SHA-256 |
| `scripts/export_schema.py` | Regenera el JSON Schema |
| `docs/adr/` | Decisiones de arquitectura |

## Ejecutar en local

Requiere Python 3.13.

```powershell
py -3.13 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r ingest_api\requirements-dev.txt -r station\requirements-dev.txt

# Una clave por línea de salida: guarda STATION_KEY para la estación y usa SHA256 en el API.
.\.venv\Scripts\python.exe scripts\gen_station_key.py

$env:STATION_KEY_HASHES = '{"GDL-001": ["<sha256>"]}'
cd ingest_api
..\.venv\Scripts\python.exe -m uvicorn app.main:create_app --factory --port 8000
```

- Documentación interactiva: http://127.0.0.1:8000/docs
- Variables de entorno del API: ver `.env.example`.

### Tres estaciones

Genera una clave por estación (`scripts\gen_station_key.py`, tres veces). Arranca el API con los tres hashes:

```powershell
$env:STATION_KEY_HASHES = '{"GDL-001": ["<sha256-1>"], "GDL-002": ["<sha256-2>"], "GDL-003": ["<sha256-3>"]}'
```

Luego abre una terminal por estación y entrega a cada una solo su clave:

```powershell
cd station
$env:STATION_ID = "GDL-001"          # GDL-002 / GDL-003 en las otras terminales
$env:STATION_KEY = "<clave en claro de GDL-001>"
..\.venv\Scripts\python.exe -m station.main
```

- **Detener:** Ctrl+C. La estación intenta vaciar el búfer durante 10 s como máximo e imprime un resumen JSON (`event: summary`). Ctrl+C se probó solo a nivel del manejador de señales; el cierre real de un proceso se demostró con CTRL_BREAK (ver ADR 0002).
- **Demo rápida:** `$env:REPLAY_HOURS = "2"; $env:REPLAY_DURATION_MIN = "0"` envía las últimas 2 h al arrancar y sigue en vivo.
- **Demo de 422:** `$env:FAULT_RATE = "0.3"`. Cada falla se registra como `fault_confirmed` o `fault_mismatch`.
- **Configuración inválida:** la estación no arranca, muestra el motivo (nunca la clave) y sale con código 2.

#### Variables de entorno de la estación

Todas están en [`station/.env.example`](station/.env.example), con sus valores por defecto.

| Variable | Defecto | Valores |
|---|---|---|
| `STATION_ID` | — (obligatoria) | `^[A-Z]{3}-[0-9]{3}$` |
| `STATION_KEY` | — (obligatoria, secreto) | 16–256 caracteres ASCII visibles, sin espacios |
| `INGEST_URL` | `http://127.0.0.1:8000/v1/readings` | URL http(s), sin credenciales |
| `INTERVAL_S` | `60` | Entero de 10 a 3600 |
| `SEED` | Entero derivado de `sha256(STATION_ID)` | Entero ≥ 0 |
| `LAT`, `LON` | `20.67`, `-103.35` | −90 a 90, −180 a 180 |
| `ELEVATION_M` | `1550` | −400 a 5000 |
| `TZ_OFFSET_H` | `-6` | −12 a 14 (offset fijo, sin horario de verano) |
| `PREVAILING_WIND_DEG` | `250` | Entero de 0 a 359 |
| `REPLAY_HOURS` | `0` (apagado) | Entero de 0 a 167 |
| `REPLAY_DURATION_MIN` | `2` | 0 (lo más rápido posible) a 1440 |
| `FAULT_RATE` | `0` | 0.0 a 1.0 |
| `BUFFER_MAX` | `1000` | Entero de 1 a 1,000,000; al llenarse se descarta la lectura más vieja |
| `MAX_READINGS` | `0` (sin límite) | Entero ≥ 0; termina al entregar o descartar esa cantidad |
| `HTTP_CONNECT_TIMEOUT_S`, `HTTP_READ_TIMEOUT_S` | `5`, `10` | Mayor que 0, máximo 120 |

## Docker

Requiere Docker Desktop (o Docker Engine con Compose v2).

```powershell
copy .env.example .env   # rellena STATION_KEY_HASHES y STATION_KEY_GDL_00N (gen_station_key.py, 3 veces)
docker compose up --build
```

- El API queda en http://127.0.0.1:8000 (Swagger en `/docs`). Las lecturas se guardan en el volumen `readings`.
- Las variables de la estación (`INTERVAL_S`, `REPLAY_HOURS`, `FAULT_RATE`, `MAX_READINGS`…) también se toman de `.env`.
- `docker compose down -v` borra el volumen.

### Prueba de estrés

```powershell
.\.venv\Scripts\python.exe scripts\stress_docker.py --stations 10 --intruders 2 --hours 24 --fault-rate 0.3
```

Lanza N estaciones concurrentes (con fallas inyectadas), intrusos con claves no registradas (401) y un suplantador (403) contra un solo API. Genera las claves en memoria y cruza el log del API, el resumen de cada estación y el JSONL. Resultados en `data/stress/<run>/results.json`; el código de salida es 0 solo si todas las comprobaciones pasan.

## Pruebas

Cada suite se corre desde su carpeta: `ingest_api/tests` y `station/tests` chocarían como paquete `tests` en una misma sesión.

```powershell
cd ingest_api;     ..\.venv\Scripts\python.exe -m pytest; cd ..
cd station;        ..\.venv\Scripts\python.exe -m pytest; cd ..
cd contract_tests; ..\.venv\Scripts\python.exe -m pytest; cd ..
```

`station/tests/test_weather_model.py` simula 24 h con 20 semillas, 2 fechas e intervalos de 60, 10 y 300 s: tarda alrededor de 1 min en un equipo normal (51–67 s medidos; más si el CPU está limitado).

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
