# Reporte — Fase A: contrato y API de ingesta

**Proyecto:** Skilliket — TEC de Monterrey GDA
**Fecha:** 2026-10-01
**Alcance:** solo Docker local; sin commit ni remoto.

La Fase A está terminada:

- Las 14 verificaciones dieron el resultado esperado contra un uvicorn real.
- pytest pasa con 83 de 83.
- El orden 401 → 415 → 413 → 400 → 422 → 403 → 409 → 201 no se puede lograr con el mecanismo estándar de FastAPI, así que el endpoint lee el cuerpo a mano (hallazgo 1).
- No se crearon `station/`, Dockerfile, docker-compose ni diagramas.

---

## Versiones

- **Python 3.13.5** (`py -3.13`), la misma versión menor que `python:3.13-slim`. El 3.14.3 local no se usó.
- **fastapi 0.142.2, pydantic 2.13.5** (pydantic_core 2.46.5), **uvicorn 0.54.0** y starlette 1.7.0. La lista completa de versiones exactas está en `ingest_api/requirements*.txt`; un `pip install --dry-run` resuelve los mismos 24 paquetes y coincide con `pip freeze`.
- Diferencias que pueden importar con la imagen slim:
  - `colorama` solo se instala en Windows (marcado con `sys_platform == "win32"`).
  - No se comprobó que las dependencias se instalen bien en Linux; queda para la Fase C. El daemon de Docker sigue apagado.

---

## Hallazgos (no se aplicaron en silencio)

1. **El orden aprobado no se puede lograr con el mecanismo estándar de FastAPI.** En `fastapi/routing.py:438-506`, cuando el cuerpo se declara como parámetro, FastAPI lo lee completo **sin tope** y lo parsea **antes** de la autenticación: un JSON roto sin clave daría 400, no 401.
   - Por eso `POST /v1/readings` lee el cuerpo a mano: un flujo con tope de 4096 bytes, luego `json.loads` para dar línea y columna, luego validación con Pydantic.
   - El handler de `RequestValidationError` sí está sobrescrito (400 para JSON mal formado, 422 para esquema inválido), pero `/v1/readings` no pasa por él. Tiene su propia prueba con una ruta auxiliar que solo existe dentro del test.
   - Queda documentado en el ADR.
2. **El índice de duplicados no aguanta un año en la configuración más chica de Container Apps.** Se midieron ~194 bytes por clave: 5 estaciones a 1 lectura/min suman **≈ 510 MB al año**, más que los 0.5 GiB de esa configuración. La mitigación (descartar las claves de más de 7 días) está en el ADR, pero **no se aplicó**. Requiere decisión.
3. **Este almacenamiento solo funciona con un proceso.** El lock y el índice viven en memoria, así que en Azure requeriría `maxReplicas = 1`. Está documentado.
4. **Efectos del modo `strict`:**
   - Se rechaza `60.0` en campos enteros (`interval_s`, `sequence`, `wind_direction_deg`).
   - En el archivo, los enteros enviados a campos decimales se guardan como decimales (`58` queda como `58.0`).
5. **Starlette 1.7 marca `httpx` como obsoleto para su TestClient.** En las dependencias de pruebas se usa `httpx2` 2.13.1 (repositorio `pydantic/httpx2`), que es lo que Starlette importa primero.
6. **Caso raro:** un JSON con un carácter Unicode inválido (por ejemplo `"\ud800"`) es sintácticamente válido, así que da **422** con `field="(body)"`, no 400.
7. **Logs en ASCII.** La consola de este Python usa cp1252, así que los logs escapan los caracteres no ASCII para que ninguno los rompa.

---

## Evidencia

### 1. pytest

Corrida `-v` completa:

```
platform win32 -- Python 3.13.5, pytest-9.1.1, pluggy-1.6.0
collected 82 items
tests/test_auth.py::test_both_station_keys_resolve_to_station PASSED
tests/test_auth.py::test_unknown_or_missing_key_is_rejected[None] PASSED
tests/test_auth.py::test_unknown_or_missing_key_is_rejected[] PASSED
tests/test_auth.py::test_unknown_or_missing_key_is_rejected[k4] PASSED
tests/test_auth.py::test_wrong_hash_rejects_its_key PASSED
tests/test_auth.py::test_parse_valid_hashes PASSED
tests/test_auth.py::test_invalid_hash_config_is_rejected[not-json|empty|not-object|bad-station-id|no-hashes|three-hashes|uppercase-hex|short-hash|repeated-in-station|shared-across-stations|not-list] PASSED (11)
tests/test_auth.py::test_settings_require_station_hashes PASSED
tests/test_auth.py::test_settings_from_env PASSED
tests/test_gen_station_key.py::test_generated_key_is_32_bytes_urlsafe_and_hash_matches_api PASSED
tests/test_models.py::test_exported_schema_matches_model PASSED
tests/test_models.py::test_valid_payload_parses PASSED
tests/test_models.py::test_minimal_payload_without_optional_fields PASSED
tests/test_models.py::test_direction_is_not_tied_to_speed[calm-null-direction] PASSED
tests/test_models.py::test_direction_is_not_tied_to_speed[low-speed-numeric-direction] PASSED
tests/test_models.py::test_gust_equal_to_speed_is_valid PASSED
tests/test_models.py::test_invalid_payloads[rh-101|temp-minus-41|gust-below-speed|extra-field|dew-point-present|missing-required|timestamp-10min-future|timestamp-8days-past|timestamp-offset-not-z|station-id-format|schema-version-2|string-in-number|bool-in-number|interval-too-short|negative-sequence|direction-360|device-extra] PASSED (17)
tests/test_models.py::test_window_uses_validation_context PASSED
tests/test_readings.py::test_valid_payload_is_created_and_persisted PASSED
tests/test_readings.py::test_duplicate_is_409 PASSED
tests/test_readings.py::test_duplicate_detection_uses_instant_not_text PASSED
tests/test_readings.py::test_index_is_rebuilt_after_restart PASSED
tests/test_readings.py::test_low_wind_direction_cases_are_created[null-direction|numeric-direction] PASSED (2)
tests/test_readings.py::test_missing_key_is_401 PASSED
tests/test_readings.py::test_invalid_key_is_401 PASSED
tests/test_readings.py::test_both_keys_of_a_station_are_accepted PASSED
tests/test_readings.py::test_key_of_other_station_is_403 PASSED
tests/test_readings.py::test_unsupported_content_type_is_415[text/plain|application/xml|application/json; charset=latin-1] PASSED (3)
tests/test_readings.py::test_json_with_utf8_charset_is_accepted PASSED
tests/test_readings.py::test_body_over_4kb_is_413 PASSED
tests/test_readings.py::test_chunked_body_over_4kb_without_content_length_is_413 PASSED
tests/test_readings.py::test_body_of_exactly_4096_bytes_is_not_413 PASSED
tests/test_readings.py::test_malformed_json_is_400_with_position[missing-value|empty-body|nan] PASSED (3)
tests/test_readings.py::test_invalid_utf8_is_400 PASSED
tests/test_readings.py::test_deeply_nested_json_is_400 PASSED
tests/test_readings.py::test_schema_error_is_422_with_field_errors PASSED
tests/test_readings.py::test_missing_field_error_does_not_echo_payload PASSED
tests/test_readings.py::test_non_object_body_is_422 PASSED
tests/test_readings.py::test_timestamp_window_comes_from_settings PASSED
tests/test_readings.py::test_order_401_before_415_413_400 PASSED
tests/test_readings.py::test_order_415_before_413 PASSED
tests/test_readings.py::test_order_413_before_400 PASSED
tests/test_readings.py::test_order_422_before_403 PASSED
tests/test_readings.py::test_order_403_before_409 PASSED
tests/test_readings.py::test_unexpected_error_is_generic_500 PASSED
tests/test_readings.py::test_health_docs_and_openapi PASSED
tests/test_readings.py::test_logs_have_ids_and_never_the_key_or_payload PASSED
tests/test_storage.py::test_concurrent_distinct_saves_write_whole_lines PASSED
tests/test_storage.py::test_concurrent_same_reading_saves_once PASSED
tests/test_storage.py::test_index_rebuild_skips_corrupt_lines_and_keeps_appending_cleanly PASSED
tests/test_storage.py::test_record_format PASSED
============================= 82 passed in 7.31s ==============================
```

Los casos parametrizados están agrupados en una línea cada uno, con el conteo entre paréntesis; cada caso pasó por separado. Después se agregó la prueba del handler. Corrida final:

```
tests/test_readings.py::test_request_validation_handler_separates_400_and_422 PASSED [ 91%]
============================= 83 passed in 7.09s ==============================
```

### 2–13. Contra uvicorn real

Servidor en `127.0.0.1:8765`, con claves generadas por `gen_station_key.py` que solo vivieron en memoria.

```
===== 2. Payload valido -> 201 y linea en el archivo =====
[valido] -> 201 application/json
    {"status":"created","reading_id":"8135b48a-df75-4de8-a220-4ec5e9e31102","station_id":"GDL-001","received_at":"2026-10-01T23:36:25.649220Z"}
    Linea en data/readings.jsonl:
    {"reading_id":"8135b48a-df75-4de8-a220-4ec5e9e31102","received_at":"2026-10-01T23:36:25.649220Z","reading":{"schema_version":"1.0","station_id":"GDL-001","timestamp":"2026-10-01T23:36:16Z","sequence":1,"interval_s":60,"measurements":{"air_temperature_c":24.7,"relative_humidity_pct":58.0,"station_pressure_hpa":841.3,"wind_speed_ms":3.4,"wind_gust_ms":5.1,"wind_direction_deg":245,"precipitation_mm":0.0,"solar_radiation_wm2":612.0},"device":{"battery_v":13.21,"firmware":"sim-0.1.0"}}}
===== 3. Mismo payload otra vez -> 409 =====
[duplicado] -> 409 application/problem+json
    {"type":"urn:skilliket:problem:duplicate-reading","title":"Conflict","status":409,"detail":"Ya existe una lectura con este station_id y timestamp; al reintentar, trátalo como éxito","instance":"/v1/readings"}
===== 4. Autenticacion =====
[sin clave] -> 401 application/problem+json
    {"type":"urn:skilliket:problem:unauthorized","title":"Unauthorized","status":401,"detail":"Falta X-Station-Key o no es válida","instance":"/v1/readings"}
[clave invalida] -> 401 application/problem+json  (mismo cuerpo)
[clave GDL-002, cuerpo GDL-001] -> 403 application/problem+json
    {"type":"urn:skilliket:problem:forbidden","title":"Forbidden","status":403,"detail":"La clave no corresponde al station_id del cuerpo","instance":"/v1/readings"}
===== 5. Dos claves de la misma estacion; hash incorrecto =====
[GDL-001 clave 1] -> 201  {"status":"created","reading_id":"0dd94a2b-...","station_id":"GDL-001",...}
[GDL-001 clave 2] -> 201  {"status":"created","reading_id":"e3aacf89-...","station_id":"GDL-001",...}
[GDL-003 con hash registrado incorrecto] -> 401 application/problem+json
===== 6. Content-Type text/plain -> 415 =====
[text/plain] -> 415 {"type":"urn:skilliket:problem:unsupported-media-type",...,"detail":"Content-Type debe ser application/json (UTF-8)"}
===== 7. Cuerpo > 4 KB -> 413 =====
    tamano del cuerpo: 5420 bytes
[con Content-Length] -> 413 {"type":"urn:skilliket:problem:payload-too-large",...,"detail":"El cuerpo supera 4096 bytes"}
    cabeceras enviadas por httpx2 (sin la clave): {'host': '127.0.0.1:8765', 'accept': '*/*', 'accept-encoding': 'gzip, deflate', 'connection': 'keep-alive', 'user-agent': 'python-httpx2/2.13.1', 'content-type': 'application/json', 'transfer-encoding': 'chunked'}
[httpx2 chunked sin Content-Length] -> 413
[curl.exe -H 'Transfer-Encoding: chunked' --data-binary @-] -> {...payload-too-large...}  HTTP 413
===== 8. JSON mal formado -> 400 con linea/columna =====
[valor faltante en linea 3] -> 400 {"type":"urn:skilliket:problem:malformed-json","title":"Malformed JSON","status":400,"detail":"Expecting value","instance":"/v1/readings","line":3,"column":16}
[objeto sin cerrar] -> 400 {...,"detail":"Expecting ',' delimiter",...,"line":1,"column":50}
===== 9. Casos 422 =====
[HR 101] -> 422 field=measurements.relative_humidity_pct code=less_than_equal value=101
[temperatura -41] -> 422 field=measurements.air_temperature_c code=greater_than_equal value=-41
[rafaga < velocidad] -> 422 field=measurements.wind_gust_ms code=gust_less_than_speed value=5.0
[campo extra] -> 422 field=campo_extra code=extra_forbidden value=1
[dew_point_c presente] -> 422 field=measurements.dew_point_c code=extra_forbidden value=15.9
[obligatorio faltante] -> 422 field=measurements.station_pressure_hpa code=missing value=None
[timestamp +10 min] -> 422 field=timestamp code=timestamp_in_future value='2026-10-01T23:46:26Z'
[timestamp -8 dias] -> 422 field=timestamp code=timestamp_too_old value='2026-09-23T23:36:26Z'
[station_id invalido] -> 422 field=station_id code=string_pattern_mismatch value='gdl-1'
[schema_version 2.0] -> 422 field=schema_version code=literal_error value='2.0'
[string en numerico] -> 422 field=measurements.air_temperature_c code=float_type value='24.7'
    Cuerpo completo (HR 101): {"type":"urn:skilliket:problem:validation-error","title":"Validation failed","status":422,"detail":"El payload no cumple el esquema v1 (1 error(es))","instance":"/v1/readings","errors":[{"field":"measurements.relative_humidity_pct","code":"less_than_equal","message":"Input should be less than or equal to 100","value":101}]}
===== 10. Casos validos limite =====
[velocidad 0, direccion null] -> 201
[velocidad 0.2, direccion 180] -> 201
===== 11. Reinicio del servidor y payload ya guardado -> 409 =====
    servidor detenido (codigo 1); lineas en archivo: 5
    servidor reiniciado, pid 15624
[payload del paso 2 tras reinicio] -> 409 application/problem+json
===== 12. Concurrencia =====
    50 distintas: codigos={201: 50}, lineas nuevas=50, JSON valido=50/50, reading_id unicos=50
    archivo completo: 55 lineas, todas parsean: True, termina en salto de linea: True
    mismo payload x20: 201=1, 409=19, otros=[], lineas nuevas=1
===== 13. /health, /docs, /openapi.json =====
[GET /health sin clave] -> 200 {"status":"ok"}
[GET /docs] -> 200 text/html; charset=utf-8, contiene swagger-ui: True
[GET /openapi.json] -> 200 openapi=3.1.0 paths=['/health', '/v1/readings']
    responses=['201', '400', '401', '403', '409', '413', '415', '422'] security=[{'StationKey': []}]
    requestBody={'$ref': '#/components/schemas/WeatherReading'} schemas=['Device', 'FieldError', 'Measurements', 'Problem', 'ReadingCreated', 'WeatherReading']
===== Muestra del log del servidor =====
    {"ts": "2026-10-01T23:36:25.655951Z", "level": "INFO", "logger": "skilliket.ingest", "msg": "reading_created", "event": "reading", "reading_id": "8135b48a-df75-4de8-a220-4ec5e9e31102", "station_id": "GDL-001", "status": 201, "reason": "created"}
    {"ts": "2026-10-01T23:36:25.686218Z", "level": "WARNING", ..., "msg": "reading_rejected", "station_id": "GDL-001", "status": 409, "reason": "duplicate"}
    {"ts": "2026-10-01T23:36:25.712527Z", "level": "WARNING", ..., "station_id": null, "status": 401, "reason": "invalid_or_missing_key"}
```

Algunos cuerpos repetidos e identificadores están abreviados con `...`; el resto está tal cual salió. En el paso 11, el "código 1" se debe a que en Windows `terminate()` mata el proceso de golpe. Así, la prueba simula una caída y el índice se reconstruye solo a partir del archivo.

### 14. Búsqueda de claves en claro

Búsqueda de las 4 claves con `grep -F -c -f <claves>`:

```
archivos versionables (git ls-files --cached --others --exclude-standard): 26
  .env.example:0  .gitignore:0  README.md:0  docs/adr/0001-ingesta-y-persistencia.md:0
  ingest_api/app/{__init__,auth,config,errors,log,main,models,storage}.py:0
  ingest_api/{pytest.ini,requirements-dev.txt,requirements.txt}:0
  ingest_api/tests/{__init__,conftest,test_auth,test_gen_station_key,test_models,test_readings,test_storage}.py:0
  propuesta-diseno-estacion-meteorologica.md:0  schema/weather_reading.v1.schema.json:0
  scripts/{export_schema,gen_station_key}.py:0
  (exit 1; 1 = sin coincidencias)
data/server.log:0   data/readings.jsonl:0   (exit 1)
(control payload) grep -c '841.3' data/server.log -> 0
```

Las pruebas tampoco tienen claves fijas: `conftest.py` las genera en cada ejecución.

---

## Limpieza

```
ls: cannot access 'data/readings.jsonl': No such file or directory
ls: cannot access 'data': No such file or directory
ls: cannot access 'ingest_api/.pytest_tmp': No such file or directory
*.jsonl fuera de .venv: 0
pytest-of-orizo en TEMP: 0 elementos
```

Se borró `data/` completa: el JSONL, `server.log` y el script de verificación `verify_live.py`, que se creó ahí porque Bash no aceptó el heredoc.

---

## No verificado, y cambios fuera de la carpeta

- **No verificado:**
  - la imagen Docker y la instalación en Linux (Fase C);
  - el comportamiento con varios workers (no está soportado a propósito);
  - la durabilidad de `fsync` ante un corte de luz.
- **Fuera de SKILLIKET:**
  - Las dos primeras corridas de pytest crearon `%TEMP%\pytest-of-orizo\pytest-0` y `pytest-1` con datos de prueba. Se borraron y se configuró `--basetemp=.pytest_tmp` (ignorada por git) para que no se repita. Queda vacía la carpeta `pytest-of-orizo`; no se sabe si existía antes, así que no se tocó.
  - pip usa su caché habitual en `%LOCALAPPDATA%`.
- **OneDrive:** `.venv/` está dentro de la carpeta sincronizada, y OneDrive va a sincronizar miles de archivos. Conviene excluirla.

---

## git status

```
On branch master

No commits yet

Untracked files:
  (use "git add <file>..." to include in what will be committed)
	.env.example
	.gitignore
	README.md
	docs/adr/0001-ingesta-y-persistencia.md
	ingest_api/app/__init__.py
	ingest_api/app/auth.py
	ingest_api/app/config.py
	ingest_api/app/errors.py
	ingest_api/app/log.py
	ingest_api/app/main.py
	ingest_api/app/models.py
	ingest_api/app/storage.py
	ingest_api/pytest.ini
	ingest_api/requirements-dev.txt
	ingest_api/requirements.txt
	ingest_api/tests/__init__.py
	ingest_api/tests/conftest.py
	ingest_api/tests/test_auth.py
	ingest_api/tests/test_gen_station_key.py
	ingest_api/tests/test_models.py
	ingest_api/tests/test_readings.py
	ingest_api/tests/test_storage.py
	propuesta-diseno-estacion-meteorologica.md
	schema/weather_reading.v1.schema.json
	scripts/export_schema.py
	scripts/gen_station_key.py

nothing added to commit but untracked files present (use "git add" to track)
```

- Remotos: 0.
- Ignorados (`git status --ignored`): `.venv/`, `ingest_api/.pytest_cache/`, `ingest_api/app/__pycache__/` e `ingest_api/tests/__pycache__/`.
- Este reporte (`reporte-fase-a.md`) se creó después de esa salida, así que ahora también aparece como archivo sin seguimiento.

---

## Archivos creados en la Fase A (25)

`propuesta-diseno-estacion-meteorologica.md` es de la entrega anterior y no se modificó.

- `.gitignore`, `.env.example`, `README.md`
- `docs/adr/0001-ingesta-y-persistencia.md`
- `schema/weather_reading.v1.schema.json`
- `scripts/gen_station_key.py`, `scripts/export_schema.py`
- `ingest_api/requirements.txt`, `ingest_api/requirements-dev.txt`, `ingest_api/pytest.ini`
- `ingest_api/app/`: `__init__.py`, `config.py`, `models.py`, `auth.py`, `errors.py`, `log.py`, `storage.py`, `main.py`
- `ingest_api/tests/`: `__init__.py`, `conftest.py`, `test_models.py`, `test_readings.py`, `test_auth.py`, `test_storage.py`, `test_gen_station_key.py`

`app/log.py` es un módulo extra que no estaba en la estructura propuesta: contiene el formateador de logs JSON. Además de `.env.example`, el repo no tiene otro archivo de configuración.
