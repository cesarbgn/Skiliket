# Reporte Fase B — estación meteorológica simulada

**Fecha:** 2026-10-01
**Alcance:** generador, inyección de fallas y emisor de `station/`, más la suite de contrato `contract_tests/`. Sin Docker, sin diagramas y sin cambios en `ingest_api/app`.
**Entorno:** Windows 11, Python 3.13.5 (venv local `.venv`), uvicorn 0.54.0 real, procesos reales de estación.
**Decisiones:** [ADR 0002](../adr/0002-estacion-simulada.md).

## Resumen

| # | Verificación | Resultado |
|---|---|---|
| 1 | Tres suites (station, ingest_api, contract_tests) | 151 + 98 + 12 = 261 pasan; salidas completas al final |
| 2 | 3 estaciones en vivo, `INTERVAL_S=10`, `MAX_READINGS=6` | 18 líneas, 18 pares únicos, códigos de salida 0 |
| 3 | Réplica de 2 h y repetición | 120 líneas por estación; la repetición da 120 × 409; tras cambiar el minuto, 1 nuevo = 1 línea; 363 líneas = 363 pares |
| 4 | Fallas, `FAULT_RATE=0.3`, 60 lecturas × 3 estaciones | 55 inyectadas, 55 `fault_confirmed`, 0 `fault_mismatch`; los huecos de `sequence` = fallas |
| 5 | Caída del API de 50 s con `INTERVAL_S=10` | 6 reintentos; 18 de 18 en orden, sin pérdidas ni duplicados |
| 6 | Clave incorrecta / `station_id` ajeno | 401 y 403 con ERROR, 0 reintentos |
| 7 | Cierre ordenado | `CTRL_BREAK_EVENT` → resumen, `reason: signal`, código 0. SIGINT real no demostrado (ver punto 7) |
| 8 | `grep -F` de las 3 claves | 0 coincidencias (control positivo: 3 en el archivo de claves) |
| 9 | Payload real | Sin artefactos de flotante; enteros como enteros |

**Bug hallado durante la verificación y corregido:**
- **Síntoma:** en la primera corrida del punto 9 (réplica de 24 h con `REPLAY_DURATION_MIN=0`), la estación envió 549 de 1,440 lecturas, a partir de `sequence` 440.
- **Causa:** el bucle generaba todas las lecturas vencidas antes de enviar, así que desbordó `BUFFER_MAX=1000` y descartó las 440 más viejas. Además, `MAX_READINGS` cortaba al generar la última, y el vaciado de 10 s no alcanzó para el resto.
- **Corrección:** contrapresión en la réplica (la lectura siguiente se genera con el búfer vacío), y `MAX_READINGS` termina al entregar todo. Se añadieron 3 pruebas de regresión.
- **Repetición:** con el código corregido se repitieron los puntos 2 a 7 y 9. Los resultados de abajo son de esa corrida.

## 2. Tres estaciones en vivo

Claves generadas con `scripts/gen_station_key.py` (una por estación); el API recibió solo los hashes. JSONL: `data/verify-b/live.jsonl`.

```
estaciones terminaron en 42.5 s; exit codes: [0, 0, 0]
JSONL data\verify-b\live.jsonl: 18 líneas, 18 pares (station_id, timestamp) únicos
  por estación: {'GDL-001': 6, 'GDL-002': 6, 'GDL-003': 6}
  GDL-001: sequences [0, 1, 2, 3, 4, 5]; timestamps 2026-10-02T01:48:50Z .. 2026-10-02T01:49:40Z
  GDL-002: sequences [0, 1, 2, 3, 4, 5]; timestamps 2026-10-02T01:48:50Z .. 2026-10-02T01:49:40Z
  GDL-003: sequences [0, 1, 2, 3, 4, 5]; timestamps 2026-10-02T01:48:50Z .. 2026-10-02T01:49:40Z
{"ts": "2026-10-02T01:49:40.029086Z", "level": "INFO", "logger": "skilliket.station", "msg": "summary", "event": "summary", "station_id": "GDL-001", "reason": "max_readings", "summary": {"generated": 6, "created": 6, "duplicates": 0, "dropped_by_status": {}, "dropped_overflow": 0, "retries": 0, "faults_injected": 0, "faults_confirmed": 0, "faults_unexpected": 0, "unsent_at_exit": 0}}
{"ts": "2026-10-02T01:49:40.020331Z", "level": "INFO", "logger": "skilliket.station", "msg": "summary", "event": "summary", "station_id": "GDL-002", "reason": "max_readings", "summary": {"generated": 6, "created": 6, "duplicates": 0, "dropped_by_status": {}, "dropped_overflow": 0, "retries": 0, "faults_injected": 0, "faults_confirmed": 0, "faults_unexpected": 0, "unsent_at_exit": 0}}
{"ts": "2026-10-02T01:49:40.017595Z", "level": "INFO", "logger": "skilliket.station", "msg": "summary", "event": "summary", "station_id": "GDL-003", "reason": "max_readings", "summary": {"generated": 6, "created": 6, "duplicates": 0, "dropped_by_status": {}, "dropped_overflow": 0, "retries": 0, "faults_injected": 0, "faults_confirmed": 0, "faults_unexpected": 0, "unsent_at_exit": 0}}
```

## 3. Réplica y repetición

`INTERVAL_S=60`, `REPLAY_HOURS=2`, `REPLAY_DURATION_MIN=0`, `MAX_READINGS=120` (exactamente la réplica). Las corridas 1 y 2 son consecutivas. La 3 espera a que cambie el minuto: así hay un timestamp nuevo y se comprueba que líneas nuevas = timestamps nuevos con un valor distinto de cero.

```
--- corrida 1: 2.2 s, exit codes [0, 0, 0]
  GDL-001: lecturas esperadas 120, enviadas 120, líneas nuevas 120 (esperadas 120)
    resumen: created=120 duplicates=0 dropped={} generated=120
  GDL-002: lecturas esperadas 120, enviadas 120, líneas nuevas 120 (esperadas 120)
    resumen: created=120 duplicates=0 dropped={} generated=120
  GDL-003: lecturas esperadas 120, enviadas 120, líneas nuevas 120 (esperadas 120)
    resumen: created=120 duplicates=0 dropped={} generated=120
--- corrida 2: 2.0 s, exit codes [0, 0, 0]
  GDL-001: timestamps solapados con corridas previas = 120, nuevos = 0, líneas nuevas = 0
    resumen: created=0 duplicates=120 dropped={} generated=120
  GDL-002: timestamps solapados con corridas previas = 120, nuevos = 0, líneas nuevas = 0
    resumen: created=0 duplicates=120 dropped={} generated=120
  GDL-003: timestamps solapados con corridas previas = 120, nuevos = 0, líneas nuevas = 0
    resumen: created=0 duplicates=120 dropped={} generated=120
--- corrida 3: 2.2 s, exit codes [0, 0, 0]
  GDL-001: timestamps solapados con corridas previas = 119, nuevos = 1, líneas nuevas = 1
    resumen: created=1 duplicates=119 dropped={} generated=120
  GDL-002: timestamps solapados con corridas previas = 119, nuevos = 1, líneas nuevas = 1
    resumen: created=1 duplicates=119 dropped={} generated=120
  GDL-003: timestamps solapados con corridas previas = 119, nuevos = 1, líneas nuevas = 1
    resumen: created=1 duplicates=119 dropped={} generated=120
JSONL data\verify-b\replay.jsonl: 363 líneas, 363 pares (station_id, timestamp) únicos
  por estación: {'GDL-001': 121, 'GDL-002': 121, 'GDL-003': 121}
```

## 4. Fallas

`FAULT_RATE=0.3`, 60 lecturas por estación (réplica de 1 h a 60 s, lo más rápido posible). Las fallas `future_timestamp` dieron 422 aun en réplica, porque usan reloj actual + 10 min (ver ADR 0002, decisión 5).

```
JSONL data\verify-b\faults.jsonl: 125 líneas, 125 pares (station_id, timestamp) únicos
  por estación: {'GDL-001': 42, 'GDL-002': 39, 'GDL-003': 44}
  GDL-001: inyectadas=18 confirmed=18 mismatch=0 líneas=42 válidas esperadas=42
    huecos de sequence en JSONL: [0, 3, 6, 12, 13, 18, 21, 27, 28, 32, 35, 37, 38, 47, 49, 53, 54, 58]
    sequences con falla:        [0, 3, 6, 12, 13, 18, 21, 27, 28, 32, 35, 37, 38, 47, 49, 53, 54, 58]  -> coinciden: True
    resumen: {"generated": 60, "created": 42, "duplicates": 0, "dropped_by_status": {"422": 18}, "dropped_overflow": 0, "retries": 0, "faults_injected": 18, "faults_confirmed": 18, "faults_unexpected": 0, "unsent_at_exit": 0}
  GDL-002: inyectadas=21 confirmed=21 mismatch=0 líneas=39 válidas esperadas=39
    huecos de sequence en JSONL: [3, 5, 6, 8, 18, 23, 25, 29, 31, 34, 39, 41, 42, 48, 51, 52, 53, 55, 56, 57, 58]
    sequences con falla:        [3, 5, 6, 8, 18, 23, 25, 29, 31, 34, 39, 41, 42, 48, 51, 52, 53, 55, 56, 57, 58]  -> coinciden: True
    resumen: {"generated": 60, "created": 39, "duplicates": 0, "dropped_by_status": {"422": 21}, "dropped_overflow": 0, "retries": 0, "faults_injected": 21, "faults_confirmed": 21, "faults_unexpected": 0, "unsent_at_exit": 0}
  GDL-003: inyectadas=16 confirmed=16 mismatch=0 líneas=44 válidas esperadas=44
    huecos de sequence en JSONL: [6, 8, 12, 13, 15, 18, 21, 29, 31, 36, 40, 43, 44, 48, 49, 57]
    sequences con falla:        [6, 8, 12, 13, 15, 18, 21, 29, 31, 36, 40, 43, 44, 48, 49, 57]  -> coinciden: True
    resumen: {"generated": 60, "created": 44, "duplicates": 0, "dropped_by_status": {"422": 16}, "dropped_overflow": 0, "retries": 0, "faults_injected": 16, "faults_confirmed": 16, "faults_unexpected": 0, "unsent_at_exit": 0}
tipos de falla inyectados: {'bad_schema_version': 8, 'extra_field': 3, 'float_in_integer': 6, 'future_timestamp': 4, 'gust_below_speed': 4, 'humidity_over_100': 14, 'missing_required': 8, 'string_in_number': 3, 'temperature_below_min': 5}
```

Ejemplo de una línea `fault_confirmed` (lo esperado frente a lo recibido del API real):

```
{"ts": "2026-10-02T01:42:11.701783Z", "level": "INFO", "logger": "skilliket.station", "msg": "fault_confirmed", "event": "fault_check", "station_id": "GDL-001", "sequence": 0, "fault": "humidity_over_100", "expected": {"status": 422, "errors": [["measurements.relative_humidity_pct", "less_than_equal"]]}, "received": {"status": 422, "errors": [["measurements.relative_humidity_pct", "less_than_equal"]]}}
```

## 5. Caída del API

GDL-001 con `INTERVAL_S=10` y `MAX_READINGS=18`. El API se mata con `TerminateProcess` a los 35 s, queda caído 50 s (5 intervalos) y se reinicia con el mismo `STORAGE_PATH`.

```
[19:50:08] API arriba (pid 25532), STORAGE_PATH=data\verify-b\outage.jsonl
[19:50:43] API detenido (pid 25532, kill)
API caído 50.0 s (>= 4 intervalos de 10 s)
[19:51:35] API arriba (pid 15028), STORAGE_PATH=data\verify-b\outage.jsonl
estación terminó a los 161.3 s, exit code 0
send_retry: 6; buffered máx: 4
   retry seq=5 attempt=1 error=ConnectError delay_s=0.94 buffered=1
   retry seq=5 attempt=2 error=ConnectError delay_s=1.6 buffered=1
   retry seq=5 attempt=3 error=ConnectError delay_s=2.69 buffered=1
   retry seq=5 attempt=4 error=ConnectError delay_s=7.43 buffered=2
   retry seq=5 attempt=5 error=ConnectError delay_s=8.06 buffered=3
   retry seq=5 attempt=6 error=ConnectError delay_s=26.68 buffered=4
entregas (sequence, intento, status): [(0, 1, 201), (1, 1, 201), (2, 1, 201), (3, 1, 201), (4, 1, 201), (5, 7, 201), (6, 1, 201), (7, 1, 201), (8, 1, 201), (9, 1, 201), (10, 1, 201), (11, 1, 201), (12, 1, 201), (13, 1, 201), (14, 1, 201), (15, 1, 201), (16, 1, 201), (17, 1, 201)]
JSONL data\verify-b\outage.jsonl: 18 líneas, 18 pares (station_id, timestamp) únicos
  por estación: {'GDL-001': 18}
sequences en el JSONL, en orden de archivo: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17]
orden ascendente: True | sin pérdidas: True | sin duplicados: True
{"ts": "2026-10-02T01:52:50.025300Z", "level": "INFO", "logger": "skilliket.station", "msg": "summary", "event": "summary", "station_id": "GDL-001", "reason": "max_readings", "summary": {"generated": 18, "created": 18, "duplicates": 0, "dropped_by_status": {}, "dropped_overflow": 0, "retries": 6, "faults_injected": 0, "faults_confirmed": 0, "faults_unexpected": 0, "unsent_at_exit": 0}}
```

`buffered` se registra en cada reintento. Durante el último backoff (26.7 s) se acumularon más lecturas. Al volver el API, la 5 entró en su intento 7 y las siguientes salieron en orden en su primer intento.

## 6. Autenticación

```
--- auth401 (GDL-001 con una clave aleatoria no registrada), exit 0
   {"ts": "2026-10-02T01:52:53.057888Z", "level": "ERROR", "logger": "skilliket.station", "msg": "reading_rejected: 401, el API no reconoce STATION_KEY; revisa la clave (y que su hash esté en STATION_KEY_HASHES del API). No se reintenta.", "event": "reading", "station_id": "GDL-001", "sequence": 0, "timestamp": "2026-10-02T01:52:50Z", "status": 401, "attempt": 1, "outcome": "rejected", "reason": "check_station_key", "problem_type": "urn:skilliket:problem:unauthorized", "errors": []}
   {"ts": "2026-10-02T01:53:00.010205Z", "level": "ERROR", "logger": "skilliket.station", "msg": "reading_rejected: 401, el API no reconoce STATION_KEY; revisa la clave (y que su hash esté en STATION_KEY_HASHES del API). No se reintenta.", "event": "reading", "station_id": "GDL-001", "sequence": 1, "timestamp": "2026-10-02T01:53:00Z", "status": 401, "attempt": 1, "outcome": "rejected", "reason": "check_station_key", "problem_type": "urn:skilliket:problem:unauthorized", "errors": []}
   send_retry: 0
   {"summary": {"generated": 2, "created": 0, "duplicates": 0, "dropped_by_status": {"401": 2}, "dropped_overflow": 0, "retries": 0, ...}}
--- auth403 (station_id GDL-002 con la clave de GDL-001), exit 0
   {"ts": "2026-10-02T01:52:53.032961Z", "level": "ERROR", "logger": "skilliket.station", "msg": "reading_rejected: 403, la clave pertenece a otra estación; revisa STATION_ID y STATION_KEY. No se reintenta.", "event": "reading", "station_id": "GDL-002", "sequence": 0, "timestamp": "2026-10-02T01:52:50Z", "status": 403, "attempt": 1, "outcome": "rejected", "reason": "check_station_id", "problem_type": "urn:skilliket:problem:forbidden", "errors": []}
   {"ts": "2026-10-02T01:53:00.011762Z", "level": "ERROR", "logger": "skilliket.station", "msg": "reading_rejected: 403, la clave pertenece a otra estación; revisa STATION_ID y STATION_KEY. No se reintenta.", "event": "reading", "station_id": "GDL-002", "sequence": 1, "timestamp": "2026-10-02T01:53:00Z", "status": 403, "attempt": 1, "outcome": "rejected", "reason": "check_station_id", "problem_type": "urn:skilliket:problem:forbidden", "errors": []}
   send_retry: 0
   {"summary": {"generated": 2, "created": 0, "duplicates": 0, "dropped_by_status": {"403": 2}, "dropped_overflow": 0, "retries": 0, ...}}
JSONL data\verify-b\auth.jsonl: 0 líneas
```

Una solicitud por lectura (`attempt: 1`) y ningún `send_retry`.

## 7. Cierre ordenado

GDL-003 con `INTERVAL_S=10`, sin `MAX_READINGS`, lanzada con `CREATE_NEW_PROCESS_GROUP`. A los 25 s se le envía `CTRL_BREAK_EVENT` (llega como SIGBREAK).

```
terminó 0.37 s después de CTRL_BREAK_EVENT, exit code 0
{"ts": "2026-10-02T01:53:27.114672Z", "level": "INFO", "logger": "skilliket.station", "msg": "summary", "event": "summary", "station_id": "GDL-003", "reason": "signal", "summary": {"generated": 3, "created": 3, "duplicates": 0, "dropped_by_status": {}, "dropped_overflow": 0, "retries": 0, "faults_injected": 0, "faults_confirmed": 0, "faults_unexpected": 0, "unsent_at_exit": 0}}
JSONL data\verify-b\sigint.jsonl: 3 líneas, 3 pares (station_id, timestamp) únicos
```

**Lo que no se demostró con un proceso real:**
- **SIGINT (`CTRL_C_EVENT`):** en Windows llega a todo el grupo de consola, incluida la terminal que lo envía. Solo se probó el manejador dentro del mismo proceso, con `signal.raise_signal(SIGINT)` (`test_install_signal_handlers_sets_event`).
- **SIGTERM:** en Windows, `Popen.terminate()` es `TerminateProcess` y no entrega ninguna señal.
- El manejador es el mismo para las tres señales.

## 8. Búsqueda de las claves en claro

Patrones: las 3 claves generadas para la verificación. Alcance:
- 27 logs de procesos (6,983 líneas), con los resúmenes;
- la salida de la verificación;
- los 7 JSONL;
- los 3,287 archivos del proyecto, incluidos `.venv/` y `.git/`.

El archivo de claves estaba fuera del proyecto y se usó como control positivo.

```
--- coincidencias:
0
exit grep: 1 (1 = ninguna coincidencia)
--- control positivo: grep -oF en el archivo de claves -> 3
```

## 9. Payload real (bytes enviados al API)

GDL-002 con `SEED=4` (elegida porque llueve en la ventana), réplica de 24 h a 60 s. Los cuerpos se capturaron con un hook de `httpx` sobre el cliente real de la estación.

```
cuerpos enviados: 1440
con precipitation_mm > 0: 122
BODY {"schema_version":"1.0","station_id":"GDL-002","timestamp":"2026-10-01T22:16:00Z","sequence":1227,"interval_s":60,"measurements":{"air_temperature_c":25.7,"relative_humidity_pct":94,"station_pressure_hpa":841.5,"wind_speed_ms":4.5,"wind_gust_ms":8.3,"wind_direction_deg":235,"precipitation_mm":1.0,"solar_radiation_wm2":99},"device":{"battery_v":13.61,"firmware":"sim-0.1.0"}}
BODY {"schema_version":"1.0","station_id":"GDL-002","timestamp":"2026-10-01T22:17:00Z","sequence":1228,"interval_s":60,"measurements":{"air_temperature_c":25.5,"relative_humidity_pct":94,"station_pressure_hpa":841.4,"wind_speed_ms":3.9,"wind_gust_ms":6.3,"wind_direction_deg":229,"precipitation_mm":0.8,"solar_radiation_wm2":99},"device":{"battery_v":13.6,"firmware":"sim-0.1.0"}}
BODY {"schema_version":"1.0","station_id":"GDL-002","timestamp":"2026-10-01T22:18:00Z","sequence":1229,"interval_s":60,"measurements":{"air_temperature_c":25.3,"relative_humidity_pct":94,"station_pressure_hpa":841.5,"wind_speed_ms":5.7,"wind_gust_ms":8.2,"wind_direction_deg":228,"precipitation_mm":0.8,"solar_radiation_wm2":98},"device":{"battery_v":13.6,"firmware":"sim-0.1.0"}}
BODY {"schema_version":"1.0","station_id":"GDL-002","timestamp":"2026-10-01T01:49:00Z","sequence":0,"interval_s":60,"measurements":{"air_temperature_c":21.9,"relative_humidity_pct":62,"station_pressure_hpa":842.3,"wind_speed_ms":7.8,"wind_gust_ms":12.4,"wind_direction_deg":214,"precipitation_mm":0.0,"solar_radiation_wm2":0},"device":{"battery_v":12.8,"firmware":"sim-0.1.0"}}
valores de precipitation_mm serializados: {'0.0': 1318, '0.2': 25, '0.4': 21, '0.6': 12, '0.8': 28, '1.0': 36}
líneas en JSONL: 1440
JSONL de esa lectura: {"schema_version":"1.0","station_id":"GDL-002","timestamp":"2026-10-01T22:16:00Z","sequence":1227,"interval_s":60,"measurements":{"air_temperature_c":25.7,"relative_humidity_pct":94.0,"station_pressure_hpa":841.5,"wind_speed_ms":4.5,"wind_gust_ms":8.3,"wind_direction_deg":235,"precipitation_mm":1.0,"solar_radiation_wm2":99.0},"device":{"battery_v":13.61,"firmware":"sim-0.1.0"}}
```

**Lectura de la evidencia:**
- **Precipitación:** de las 1,440 lecturas, `precipitation_mm` solo toma 0.0, 0.2, 0.4, 0.6, 0.8 y 1.0. Nunca aparece `0.6000000000000001`.
- **Enteros:** `interval_s`, `sequence` y `wind_direction_deg` salen como enteros, sin `.0`.
- **Observación sobre el API (no se modificó):** en el JSONL, el API escribe `relative_humidity_pct` y `solar_radiation_wm2` como `94.0` y `99.0`, porque su modelo los declara `float` y normaliza al guardar. La estación los envía como enteros (`94`, `99`); ambos son válidos.

## Hallazgos y desviaciones

1. **Bug del bucle principal**, corregido (ver Resumen).
2. **`future_timestamp`** usa reloj actual + 10 min, no timestamp de la lectura + 10 min. Con la segunda opción, la falla no daría 422 en réplica ni con intervalos largos.
3. **`float_in_integer`** usa `float(INTERVAL_S)`, que es `60.0` con el intervalo por defecto.
4. **`test_main.py`** se añadió a la lista acordada de archivos de prueba.
5. **`PREVAILING_WIND_DEG`** (defecto 250) se añadió a las variables, porque la tabla aprobada pedía una dirección predominante configurable.
6. **Inercia térmica y curva diurna:** en la primera medición, Tmax caía después de las 17 h en ~10 % de los días secos. Se añadió inercia térmica (paso bajo de nubes y anomalía) y una bajada vespertina con pendiente desde el pico. Después: Tmax entre 13.6 y 15.4 h y Tmin en 5–8 h en ≥ 92 % de los días secos (abril y octubre).
7. **No se necesitó ningún cambio en el contrato** ni en `ingest_api/app`.
8. **Máquina lenta:** durante la sesión, el CPU estuvo limitado por el sistema (un bucle de referencia de 1 M de `sin` tardó entre 0.38 y 5.9 s), así que una corrida de la suite de `station` tardó 11 min 35 s. La corrida final, ya sin esa limitación, tardó 67 s.

## Salidas completas de las suites

### station

Comando: `cd station; ..\.venv\Scripts\python.exe -m pytest -v --durations=5` (la última línea, `exit N`, es el código de salida que agregó el script).

```
============================= test session starts =============================
platform win32 -- Python 3.13.5, pytest-9.1.1, pluggy-1.6.0 -- C:\Users\orizo\OneDrive\Documentos\TEC\SKILLIKET\.venv\Scripts\python.exe
cachedir: .pytest_cache
rootdir: C:\Users\orizo\OneDrive\Documentos\TEC\SKILLIKET\station
configfile: pytest.ini
testpaths: tests
plugins: anyio-4.15.1
collecting ... collected 151 items

tests/test_config.py::test_invalid_config_exits_2_without_printing_key[STATION_ID=None] PASSED [  0%]
tests/test_config.py::test_invalid_config_exits_2_without_printing_key[STATION_ID=gdl-001] PASSED [  1%]
tests/test_config.py::test_invalid_config_exits_2_without_printing_key[STATION_ID=GDL-0001] PASSED [  1%]
tests/test_config.py::test_invalid_config_exits_2_without_printing_key[STATION_ID=GDL-00a] PASSED [  2%]
tests/test_config.py::test_invalid_config_exits_2_without_printing_key[STATION_KEY=None] PASSED [  3%]
tests/test_config.py::test_invalid_config_exits_2_without_printing_key[STATION_KEY=short] PASSED [  3%]
tests/test_config.py::test_invalid_config_exits_2_without_printing_key[STATION_KEY=con espacios dentro de la clave larga] PASSED [  4%]
tests/test_config.py::test_invalid_config_exits_2_without_printing_key[STATION_KEY=\xf1\xf1\xf1\xf1\xf1\xf1\xf1\xf1\xf1\xf1\xf1\xf1\xf1\xf1\xf1\xf1\xf1\xf1\xf1\xf1] PASSED [  5%]
tests/test_config.py::test_invalid_config_exits_2_without_printing_key[INGEST_URL=ftp://127.0.0.1/v1/readings] PASSED [  5%]
tests/test_config.py::test_invalid_config_exits_2_without_printing_key[INGEST_URL=no-es-una-url] PASSED [  6%]
tests/test_config.py::test_invalid_config_exits_2_without_printing_key[INGEST_URL=http://user:pass@127.0.0.1:8000/v1/readings] PASSED [  7%]
tests/test_config.py::test_invalid_config_exits_2_without_printing_key[INGEST_URL=http://127.0.0.1:99999/v1/readings] PASSED [  7%]
tests/test_config.py::test_invalid_config_exits_2_without_printing_key[INTERVAL_S=9] PASSED [  8%]
tests/test_config.py::test_invalid_config_exits_2_without_printing_key[INTERVAL_S=3601] PASSED [  9%]
tests/test_config.py::test_invalid_config_exits_2_without_printing_key[INTERVAL_S=60.0] PASSED [  9%]
tests/test_config.py::test_invalid_config_exits_2_without_printing_key[INTERVAL_S=abc] PASSED [ 10%]
tests/test_config.py::test_invalid_config_exits_2_without_printing_key[SEED=-1] PASSED [ 11%]
tests/test_config.py::test_invalid_config_exits_2_without_printing_key[SEED=1.5] PASSED [ 11%]
tests/test_config.py::test_invalid_config_exits_2_without_printing_key[LAT=90.1] PASSED [ 12%]
tests/test_config.py::test_invalid_config_exits_2_without_printing_key[LON=-180.5] PASSED [ 13%]
tests/test_config.py::test_invalid_config_exits_2_without_printing_key[ELEVATION_M=5001] PASSED [ 13%]
tests/test_config.py::test_invalid_config_exits_2_without_printing_key[TZ_OFFSET_H=15] PASSED [ 14%]
tests/test_config.py::test_invalid_config_exits_2_without_printing_key[TZ_OFFSET_H=nan] PASSED [ 15%]
tests/test_config.py::test_invalid_config_exits_2_without_printing_key[PREVAILING_WIND_DEG=360] PASSED [ 15%]
tests/test_config.py::test_invalid_config_exits_2_without_printing_key[REPLAY_HOURS=168] PASSED [ 16%]
tests/test_config.py::test_invalid_config_exits_2_without_printing_key[REPLAY_HOURS=-1] PASSED [ 17%]
tests/test_config.py::test_invalid_config_exits_2_without_printing_key[REPLAY_DURATION_MIN=-0.5] PASSED [ 17%]
tests/test_config.py::test_invalid_config_exits_2_without_printing_key[FAULT_RATE=1.01] PASSED [ 18%]
tests/test_config.py::test_invalid_config_exits_2_without_printing_key[FAULT_RATE=-0.1] PASSED [ 19%]
tests/test_config.py::test_invalid_config_exits_2_without_printing_key[FAULT_RATE=inf] PASSED [ 19%]
tests/test_config.py::test_invalid_config_exits_2_without_printing_key[BUFFER_MAX=0] PASSED [ 20%]
tests/test_config.py::test_invalid_config_exits_2_without_printing_key[MAX_READINGS=-1] PASSED [ 21%]
tests/test_config.py::test_invalid_config_exits_2_without_printing_key[HTTP_CONNECT_TIMEOUT_S=0] PASSED [ 21%]
tests/test_config.py::test_invalid_config_exits_2_without_printing_key[HTTP_READ_TIMEOUT_S=121] PASSED [ 22%]
tests/test_config.py::test_key_pasted_in_another_variable_is_not_echoed PASSED [ 23%]
tests/test_config.py::test_defaults PASSED                               [ 23%]
tests/test_config.py::test_default_seed_is_sha256_of_station_id PASSED   [ 24%]
tests/test_config.py::test_explicit_values PASSED                        [ 25%]
tests/test_config.py::test_empty_optional_values_use_defaults PASSED     [ 25%]
tests/test_config.py::test_key_never_in_repr_or_errors PASSED            [ 26%]
tests/test_config.py::test_secret_equality_does_not_leak PASSED          [ 27%]
tests/test_faults.py::test_catalog_has_the_nine_agreed_faults PASSED     [ 27%]
tests/test_faults.py::test_rate_1_mutates_every_reading_and_covers_every_fault PASSED [ 28%]
tests/test_faults.py::test_rate_0_never_mutates PASSED                   [ 29%]
tests/test_faults.py::test_rate_0_3_proportion_over_2000 PASSED          [ 29%]
tests/test_faults.py::test_mutation_does_not_touch_original PASSED       [ 30%]
tests/test_faults.py::test_each_mutation[humidity_over_100] PASSED       [ 31%]
tests/test_faults.py::test_each_mutation[temperature_below_min] PASSED   [ 31%]
tests/test_faults.py::test_each_mutation[gust_below_speed] PASSED        [ 32%]
tests/test_faults.py::test_each_mutation[extra_field] PASSED             [ 33%]
tests/test_faults.py::test_each_mutation[missing_required] PASSED        [ 33%]
tests/test_faults.py::test_each_mutation[string_in_number] PASSED        [ 34%]
tests/test_faults.py::test_each_mutation[future_timestamp] PASSED        [ 35%]
tests/test_faults.py::test_each_mutation[bad_schema_version] PASSED      [ 35%]
tests/test_faults.py::test_each_mutation[float_in_integer] PASSED        [ 36%]
tests/test_faults.py::test_fault_rng_is_independent_from_weather_seed PASSED [ 37%]
tests/test_faults.py::test_same_seed_same_faults PASSED                  [ 37%]
tests/test_main.py::test_live_mode_aligned_timestamps_and_sequence PASSED [ 38%]
tests/test_main.py::test_live_timestamps_never_in_future PASSED          [ 39%]
tests/test_main.py::test_no_cumulative_drift_with_slow_requests PASSED   [ 39%]
tests/test_main.py::test_replay_then_live PASSED                         [ 40%]
tests/test_main.py::test_replay_is_spread_over_duration PASSED           [ 41%]
tests/test_main.py::test_fast_replay_larger_than_buffer_does_not_overflow PASSED [ 41%]
tests/test_main.py::test_replay_pauses_while_api_is_down PASSED          [ 42%]
tests/test_main.py::test_max_readings_waits_for_delivery PASSED          [ 43%]
tests/test_main.py::test_outage_buffers_and_delivers_in_order PASSED     [ 43%]
tests/test_main.py::test_signal_stop_drains_and_exits PASSED             [ 44%]
tests/test_main.py::test_drain_gives_up_after_timeout PASSED             [ 45%]
tests/test_main.py::test_faults_create_sequence_gaps PASSED              [ 45%]
tests/test_main.py::test_fault_rate_does_not_change_weather_end_to_end PASSED [ 46%]
tests/test_main.py::test_install_signal_handlers_sets_event PASSED       [ 47%]
tests/test_sender.py::test_classify[201-created] PASSED                  [ 47%]
tests/test_sender.py::test_classify[200-created] PASSED                  [ 48%]
tests/test_sender.py::test_classify[409-duplicate] PASSED                [ 49%]
tests/test_sender.py::test_classify[500-retry] PASSED                    [ 49%]
tests/test_sender.py::test_classify[502-retry] PASSED                    [ 50%]
tests/test_sender.py::test_classify[503-retry] PASSED                    [ 50%]
tests/test_sender.py::test_classify[408-retry] PASSED                    [ 51%]
tests/test_sender.py::test_classify[429-retry] PASSED                    [ 52%]
tests/test_sender.py::test_classify[400-rejected] PASSED                 [ 52%]
tests/test_sender.py::test_classify[401-rejected] PASSED                 [ 53%]
tests/test_sender.py::test_classify[403-rejected] PASSED                 [ 54%]
tests/test_sender.py::test_classify[404-rejected] PASSED                 [ 54%]
tests/test_sender.py::test_classify[413-rejected] PASSED                 [ 55%]
tests/test_sender.py::test_classify[415-rejected] PASSED                 [ 56%]
tests/test_sender.py::test_classify[422-rejected] PASSED                 [ 56%]
tests/test_sender.py::test_201_is_created PASSED                         [ 57%]
tests/test_sender.py::test_request_format PASSED                         [ 58%]
tests/test_sender.py::test_409_counts_as_delivered_duplicate PASSED      [ 58%]
tests/test_sender.py::test_503_then_201_keeps_order PASSED               [ 59%]
tests/test_sender.py::test_timeouts_and_transport_errors_are_retried[ReadTimeout] PASSED [ 60%]
tests/test_sender.py::test_timeouts_and_transport_errors_are_retried[ConnectTimeout] PASSED [ 60%]
tests/test_sender.py::test_timeouts_and_transport_errors_are_retried[RemoteProtocolError] PASSED [ 61%]
tests/test_sender.py::test_connection_down_with_5_buffered_flushes_in_order PASSED [ 62%]
tests/test_sender.py::test_backoff_is_exponential_with_jitter_and_capped PASSED [ 62%]
tests/test_sender.py::test_overflow_drops_oldest PASSED                  [ 63%]
tests/test_sender.py::test_non_retryable_are_dropped_without_retry[400] PASSED [ 64%]
tests/test_sender.py::test_non_retryable_are_dropped_without_retry[401] PASSED [ 64%]
tests/test_sender.py::test_non_retryable_are_dropped_without_retry[403] PASSED [ 65%]
tests/test_sender.py::test_non_retryable_are_dropped_without_retry[413] PASSED [ 66%]
tests/test_sender.py::test_non_retryable_are_dropped_without_retry[415] PASSED [ 66%]
tests/test_sender.py::test_non_retryable_are_dropped_without_retry[422] PASSED [ 67%]
tests/test_sender.py::test_401_does_not_loop PASSED                      [ 68%]
tests/test_sender.py::test_422_logs_field_code_summary PASSED            [ 68%]
tests/test_sender.py::test_fault_confirmed_and_mismatch PASSED           [ 69%]
tests/test_sender.py::test_fault_accepted_with_201_is_mismatch PASSED    [ 70%]
tests/test_sender.py::test_logs_never_contain_key_or_full_payload PASSED [ 70%]
tests/test_weather_model.py::test_payload_values_within_api_ranges_and_formats[60] PASSED [ 71%]
tests/test_weather_model.py::test_payload_values_within_api_ranges_and_formats[10] PASSED [ 72%]
tests/test_weather_model.py::test_payload_values_within_api_ranges_and_formats[300] PASSED [ 72%]
tests/test_weather_model.py::test_humidity_in_range_and_high_when_raining[60] PASSED [ 73%]
tests/test_weather_model.py::test_humidity_in_range_and_high_when_raining[10] PASSED [ 74%]
tests/test_weather_model.py::test_humidity_in_range_and_high_when_raining[300] PASSED [ 74%]
tests/test_weather_model.py::test_gust_and_direction[60] PASSED          [ 75%]
tests/test_weather_model.py::test_gust_and_direction[10] PASSED          [ 76%]
tests/test_weather_model.py::test_gust_and_direction[300] PASSED         [ 76%]
tests/test_weather_model.py::test_radiation_zero_at_night_and_bounded_by_clear_sky[60] PASSED [ 77%]
tests/test_weather_model.py::test_radiation_zero_at_night_and_bounded_by_clear_sky[10] PASSED [ 78%]
tests/test_weather_model.py::test_radiation_zero_at_night_and_bounded_by_clear_sky[300] PASSED [ 78%]
tests/test_weather_model.py::test_clear_sky_maximum_between_900_and_1200[2026-04-15] PASSED [ 79%]
tests/test_weather_model.py::test_clear_sky_maximum_between_900_and_1200[2026-10-01] PASSED [ 80%]
tests/test_weather_model.py::test_tmax_and_tmin_hours_on_dry_days[2026-04-15-60] PASSED [ 80%]
tests/test_weather_model.py::test_tmax_and_tmin_hours_on_dry_days[2026-04-15-10] PASSED [ 81%]
tests/test_weather_model.py::test_tmax_and_tmin_hours_on_dry_days[2026-04-15-300] PASSED [ 82%]
tests/test_weather_model.py::test_tmax_and_tmin_hours_on_dry_days[2026-10-01-60] PASSED [ 82%]
tests/test_weather_model.py::test_tmax_and_tmin_hours_on_dry_days[2026-10-01-10] PASSED [ 83%]
tests/test_weather_model.py::test_tmax_and_tmin_hours_on_dry_days[2026-10-01-300] PASSED [ 84%]
tests/test_weather_model.py::test_temperature_humidity_anticorrelated_on_dry_days[2026-04-15-60] PASSED [ 84%]
tests/test_weather_model.py::test_temperature_humidity_anticorrelated_on_dry_days[2026-04-15-10] PASSED [ 85%]
tests/test_weather_model.py::test_temperature_humidity_anticorrelated_on_dry_days[2026-04-15-300] PASSED [ 86%]
tests/test_weather_model.py::test_temperature_humidity_anticorrelated_on_dry_days[2026-10-01-60] PASSED [ 86%]
tests/test_weather_model.py::test_temperature_humidity_anticorrelated_on_dry_days[2026-10-01-10] PASSED [ 87%]
tests/test_weather_model.py::test_temperature_humidity_anticorrelated_on_dry_days[2026-10-01-300] PASSED [ 88%]
tests/test_weather_model.py::test_precipitation_has_no_float_artifacts PASSED [ 88%]
tests/test_weather_model.py::test_same_seed_is_byte_identical[60] PASSED [ 89%]
tests/test_weather_model.py::test_same_seed_is_byte_identical[10] PASSED [ 90%]
tests/test_weather_model.py::test_same_seed_is_byte_identical[300] PASSED [ 90%]
tests/test_weather_model.py::test_different_seeds_and_stations_differ PASSED [ 91%]
tests/test_weather_model.py::test_fault_rate_does_not_change_unmutated_measurements PASSED [ 92%]
tests/test_weather_model.py::test_precipitation_accumulates_per_interval[10] PASSED [ 92%]
tests/test_weather_model.py::test_precipitation_accumulates_per_interval[60] PASSED [ 93%]
tests/test_weather_model.py::test_precipitation_accumulates_per_interval[300] PASSED [ 94%]
tests/test_weather_model.py::test_precipitation_accumulates_per_interval[3600] PASSED [ 94%]
tests/test_weather_model.py::test_ar1_variance_and_correlation_do_not_depend_on_dt[10] PASSED [ 95%]
tests/test_weather_model.py::test_ar1_variance_and_correlation_do_not_depend_on_dt[60] PASSED [ 96%]
tests/test_weather_model.py::test_ar1_variance_and_correlation_do_not_depend_on_dt[300] PASSED [ 96%]
tests/test_weather_model.py::test_ar1_variance_and_correlation_do_not_depend_on_dt[3600] PASSED [ 97%]
tests/test_weather_model.py::test_battery_charges_by_day_and_drains_at_night PASSED [ 98%]
tests/test_weather_model.py::test_magnus_relative_humidity PASSED        [ 98%]
tests/test_weather_model.py::test_generator_sequence_and_timestamp PASSED [ 99%]
tests/test_weather_model.py::test_model_requires_advancing_aware_time PASSED [100%]

============================= slowest 5 durations =============================
39.56s call     tests/test_weather_model.py::test_payload_values_within_api_ranges_and_formats[10]
7.62s call     tests/test_weather_model.py::test_payload_values_within_api_ranges_and_formats[60]
1.34s call     tests/test_weather_model.py::test_payload_values_within_api_ranges_and_formats[300]
0.73s call     tests/test_config.py::test_invalid_config_exits_2_without_printing_key[PREVAILING_WIND_DEG=360]
0.73s call     tests/test_config.py::test_invalid_config_exits_2_without_printing_key[INGEST_URL=no-es-una-url]
======================= 151 passed in 67.50s (0:01:07) ========================
exit 0
```

### ingest_api

Comando: `cd ingest_api; ..\.venv\Scripts\python.exe -m pytest -v` (la última línea, `exit N`, es el código de salida que agregó el script).

```
============================= test session starts =============================
platform win32 -- Python 3.13.5, pytest-9.1.1, pluggy-1.6.0 -- C:\Users\orizo\OneDrive\Documentos\TEC\SKILLIKET\.venv\Scripts\python.exe
cachedir: .pytest_cache
rootdir: C:\Users\orizo\OneDrive\Documentos\TEC\SKILLIKET\ingest_api
configfile: pytest.ini
testpaths: tests
plugins: anyio-4.15.1
collecting ... collected 98 items

tests/test_auth.py::test_both_station_keys_resolve_to_station PASSED     [  1%]
tests/test_auth.py::test_unknown_or_missing_key_is_rejected[None] PASSED [  2%]
tests/test_auth.py::test_unknown_or_missing_key_is_rejected[] PASSED     [  3%]
tests/test_auth.py::test_unknown_or_missing_key_is_rejected[k4] PASSED   [  4%]
tests/test_auth.py::test_wrong_hash_rejects_its_key PASSED               [  5%]
tests/test_auth.py::test_parse_valid_hashes PASSED                       [  6%]
tests/test_auth.py::test_invalid_hash_config_is_rejected[not-json] PASSED [  7%]
tests/test_auth.py::test_invalid_hash_config_is_rejected[empty] PASSED   [  8%]
tests/test_auth.py::test_invalid_hash_config_is_rejected[not-object] PASSED [  9%]
tests/test_auth.py::test_invalid_hash_config_is_rejected[bad-station-id] PASSED [ 10%]
tests/test_auth.py::test_invalid_hash_config_is_rejected[unicode-digit-station-id] PASSED [ 11%]
tests/test_auth.py::test_invalid_hash_config_is_rejected[no-hashes] PASSED [ 12%]
tests/test_auth.py::test_invalid_hash_config_is_rejected[three-hashes] PASSED [ 13%]
tests/test_auth.py::test_invalid_hash_config_is_rejected[uppercase-hex] PASSED [ 14%]
tests/test_auth.py::test_invalid_hash_config_is_rejected[short-hash] PASSED [ 15%]
tests/test_auth.py::test_invalid_hash_config_is_rejected[repeated-in-station] PASSED [ 16%]
tests/test_auth.py::test_invalid_hash_config_is_rejected[shared-across-stations] PASSED [ 17%]
tests/test_auth.py::test_invalid_hash_config_is_rejected[not-list] PASSED [ 18%]
tests/test_auth.py::test_settings_require_station_hashes PASSED          [ 19%]
tests/test_auth.py::test_settings_from_env PASSED                        [ 20%]
tests/test_gen_station_key.py::test_generated_key_is_32_bytes_urlsafe_and_hash_matches_api PASSED [ 21%]
tests/test_models.py::test_exported_schema_matches_model PASSED          [ 22%]
tests/test_models.py::test_valid_payload_parses PASSED                   [ 23%]
tests/test_models.py::test_minimal_payload_without_optional_fields PASSED [ 24%]
tests/test_models.py::test_direction_is_not_tied_to_speed[calm-null-direction] PASSED [ 25%]
tests/test_models.py::test_direction_is_not_tied_to_speed[low-speed-numeric-direction] PASSED [ 26%]
tests/test_models.py::test_gust_equal_to_speed_is_valid PASSED           [ 27%]
tests/test_models.py::test_patterns_reject_non_ascii_digits[station-id] PASSED [ 28%]
tests/test_models.py::test_patterns_reject_non_ascii_digits[timestamp] PASSED [ 29%]
tests/test_models.py::test_invalid_payloads[rh-101] PASSED               [ 30%]
tests/test_models.py::test_invalid_payloads[temp-minus-41] PASSED        [ 31%]
tests/test_models.py::test_invalid_payloads[gust-below-speed] PASSED     [ 32%]
tests/test_models.py::test_invalid_payloads[extra-field] PASSED          [ 33%]
tests/test_models.py::test_invalid_payloads[dew-point-present] PASSED    [ 34%]
tests/test_models.py::test_invalid_payloads[missing-required] PASSED     [ 35%]
tests/test_models.py::test_invalid_payloads[timestamp-10min-future] PASSED [ 36%]
tests/test_models.py::test_invalid_payloads[timestamp-8days-past] PASSED [ 37%]
tests/test_models.py::test_invalid_payloads[timestamp-offset-not-z] PASSED [ 38%]
tests/test_models.py::test_invalid_payloads[station-id-format] PASSED    [ 39%]
tests/test_models.py::test_invalid_payloads[schema-version-2] PASSED     [ 40%]
tests/test_models.py::test_invalid_payloads[string-in-number] PASSED     [ 41%]
tests/test_models.py::test_invalid_payloads[bool-in-number] PASSED       [ 42%]
tests/test_models.py::test_invalid_payloads[interval-too-short] PASSED   [ 43%]
tests/test_models.py::test_invalid_payloads[negative-sequence] PASSED    [ 44%]
tests/test_models.py::test_invalid_payloads[direction-360] PASSED        [ 45%]
tests/test_models.py::test_invalid_payloads[device-extra] PASSED         [ 46%]
tests/test_models.py::test_invalid_payloads[interval-float] PASSED       [ 47%]
tests/test_models.py::test_invalid_payloads[sequence-float] PASSED       [ 48%]
tests/test_models.py::test_invalid_payloads[station-id-unicode-digits] PASSED [ 50%]
tests/test_models.py::test_invalid_payloads[timestamp-fullwidth-digits] PASSED [ 51%]
tests/test_models.py::test_window_uses_validation_context PASSED         [ 52%]
tests/test_readings.py::test_valid_payload_is_created_and_persisted PASSED [ 53%]
tests/test_readings.py::test_duplicate_is_409 PASSED                     [ 54%]
tests/test_readings.py::test_duplicate_detection_uses_instant_not_text PASSED [ 55%]
tests/test_readings.py::test_index_is_rebuilt_after_restart PASSED       [ 56%]
tests/test_readings.py::test_key_inside_window_survives_pruning PASSED   [ 57%]
tests/test_readings.py::test_low_wind_direction_cases_are_created[null-direction] PASSED [ 58%]
tests/test_readings.py::test_low_wind_direction_cases_are_created[numeric-direction] PASSED [ 59%]
tests/test_readings.py::test_missing_key_is_401 PASSED                   [ 60%]
tests/test_readings.py::test_invalid_key_is_401 PASSED                   [ 61%]
tests/test_readings.py::test_both_keys_of_a_station_are_accepted PASSED  [ 62%]
tests/test_readings.py::test_key_of_other_station_is_403 PASSED          [ 63%]
tests/test_readings.py::test_unsupported_content_type_is_415[text/plain] PASSED [ 64%]
tests/test_readings.py::test_unsupported_content_type_is_415[application/xml] PASSED [ 65%]
tests/test_readings.py::test_unsupported_content_type_is_415[application/json; charset=latin-1] PASSED [ 66%]
tests/test_readings.py::test_json_with_utf8_charset_is_accepted PASSED   [ 67%]
tests/test_readings.py::test_body_over_4kb_is_413 PASSED                 [ 68%]
tests/test_readings.py::test_chunked_body_over_4kb_without_content_length_is_413 PASSED [ 69%]
tests/test_readings.py::test_body_of_4097_bytes_is_413 PASSED            [ 70%]
tests/test_readings.py::test_body_of_exactly_4096_bytes_is_not_413 PASSED [ 71%]
tests/test_readings.py::test_malformed_json_is_400_with_position[missing-value] PASSED [ 72%]
tests/test_readings.py::test_malformed_json_is_400_with_position[empty-body] PASSED [ 73%]
tests/test_readings.py::test_malformed_json_is_400_with_position[nan] PASSED [ 74%]
tests/test_readings.py::test_invalid_utf8_is_400 PASSED                  [ 75%]
tests/test_readings.py::test_deeply_nested_json_is_400 PASSED            [ 76%]
tests/test_readings.py::test_schema_error_is_422_with_field_errors PASSED [ 77%]
tests/test_readings.py::test_missing_field_error_does_not_echo_payload PASSED [ 78%]
tests/test_readings.py::test_non_object_body_is_422 PASSED               [ 79%]
tests/test_readings.py::test_timestamp_window_comes_from_settings PASSED [ 80%]
tests/test_readings.py::test_order_401_before_415_413_400 PASSED         [ 81%]
tests/test_readings.py::test_order_415_before_413 PASSED                 [ 82%]
tests/test_readings.py::test_order_413_before_400 PASSED                 [ 83%]
tests/test_readings.py::test_order_422_before_403 PASSED                 [ 84%]
tests/test_readings.py::test_order_403_before_409 PASSED                 [ 85%]
tests/test_readings.py::test_request_validation_handler_separates_400_and_422 PASSED [ 86%]
tests/test_readings.py::test_unknown_route_is_404_problem PASSED         [ 87%]
tests/test_readings.py::test_wrong_method_is_405_problem_with_allow PASSED [ 88%]
tests/test_readings.py::test_unexpected_error_is_generic_500 PASSED      [ 89%]
tests/test_readings.py::test_health_docs_and_openapi PASSED              [ 90%]
tests/test_readings.py::test_logs_have_ids_and_never_the_key_or_payload PASSED [ 91%]
tests/test_storage.py::test_concurrent_distinct_saves_write_whole_lines PASSED [ 92%]
tests/test_storage.py::test_concurrent_same_reading_saves_once PASSED    [ 93%]
tests/test_storage.py::test_index_rebuild_skips_corrupt_lines_and_keeps_appending_cleanly PASSED [ 94%]
tests/test_storage.py::test_partial_write_failure_does_not_glue_next_reading PASSED [ 95%]
tests/test_storage.py::test_prune_drops_keys_outside_retention PASSED    [ 96%]
tests/test_storage.py::test_prune_runs_at_most_every_5_minutes PASSED    [ 97%]
tests/test_storage.py::test_rebuild_loads_only_keys_inside_retention PASSED [ 98%]
tests/test_storage.py::test_record_format PASSED                         [100%]

============================= 98 passed in 2.08s ==============================
exit 0
```

### contract_tests

Comando: `cd contract_tests; ..\.venv\Scripts\python.exe -m pytest -v` (la última línea, `exit N`, es el código de salida que agregó el script).

```
============================= test session starts =============================
platform win32 -- Python 3.13.5, pytest-9.1.1, pluggy-1.6.0 -- C:\Users\orizo\OneDrive\Documentos\TEC\SKILLIKET\.venv\Scripts\python.exe
cachedir: .pytest_cache
rootdir: C:\Users\orizo\OneDrive\Documentos\TEC\SKILLIKET\contract_tests
configfile: pytest.ini
testpaths: .
plugins: anyio-4.15.1
collecting ... collected 12 items

test_contract.py::test_all_4320_payloads_are_valid PASSED                [  8%]
test_contract.py::test_stations_produce_different_series PASSED          [ 16%]
test_contract.py::test_each_fault_fails_with_expected_field_and_code[humidity_over_100] PASSED [ 25%]
test_contract.py::test_each_fault_fails_with_expected_field_and_code[temperature_below_min] PASSED [ 33%]
test_contract.py::test_each_fault_fails_with_expected_field_and_code[gust_below_speed] PASSED [ 41%]
test_contract.py::test_each_fault_fails_with_expected_field_and_code[extra_field] PASSED [ 50%]
test_contract.py::test_each_fault_fails_with_expected_field_and_code[missing_required] PASSED [ 58%]
test_contract.py::test_each_fault_fails_with_expected_field_and_code[string_in_number] PASSED [ 66%]
test_contract.py::test_each_fault_fails_with_expected_field_and_code[future_timestamp] PASSED [ 75%]
test_contract.py::test_each_fault_fails_with_expected_field_and_code[bad_schema_version] PASSED [ 83%]
test_contract.py::test_each_fault_fails_with_expected_field_and_code[float_in_integer] PASSED [ 91%]
test_contract.py::test_injector_rate_1_over_all_payloads_matches_catalog PASSED [100%]

============================= 12 passed in 1.45s ==============================
exit 0
```
