# ADR 0002 — Estación meteorológica simulada (generador y emisor)

- **Estado:** Aceptada
- **Fecha:** 2026-10-01
- **Alcance:** Fase B. Generador, inyección de fallas y emisor en Python 3.13, ejecutados con el venv local contra uvicorn real. Sin Docker (Fase C) ni diagramas (Fase D).

## Contexto

El API de ingesta (ADR 0001) existe y valida el contrato v1 (`ingest_api/app/models.py`, fuente de verdad). Falta la estación: un proceso por estación (GDL-001, GDL-002, GDL-003) que genere lecturas físicamente coherentes con la climatología de Guadalajara, las envíe con su clave y sobreviva a caídas del API sin perder ni duplicar datos.

## Estructura

| Ruta | Contenido |
|---|---|
| `station/station/config.py` | Variables de entorno, validación fail-fast (código de salida 2), `Secret` para la clave |
| `station/station/solar.py` | Declinación, ecuación del tiempo, elevación solar, radiación de cielo despejado |
| `station/station/weather_model.py` | `WeatherModel.step(ts)` con estado y `ReadingGenerator` (payload v1, `sequence`) |
| `station/station/faults.py` | Catálogo de 9 fallas y `FaultInjector` con su propio generador aleatorio |
| `station/station/sender.py` | `Sender` (un POST, clasificación de la respuesta) y `Outbox` (búfer FIFO, backoff, contadores) |
| `station/station/main.py` | Programación de ticks (vivo y réplica), señales, vaciado final y resumen |
| `station/station/log.py` | Logs JSON de una línea, ASCII escapado (mismo formato que el API) |
| `station/tests/` | 5 archivos de prueba; `test_main.py` se añadió a la lista acordada para separar las pruebas del bucle (ticks, réplica, señales) de las del emisor |
| `contract_tests/` | Salida del generador contra el modelo real del API, con su propio `pytest.ini` |

Cada suite se corre desde su carpeta (`station/`, `ingest_api/`, `contract_tests/`): `station/tests` e `ingest_api/tests` chocarían como paquete `tests` en una misma sesión.

**Dependencias:** `httpx==0.28.1` y sus transitivas, todas con versión exacta. Conviven en el mismo venv con `ingest_api/requirements-dev.txt`. Starlette usa `httpx2` para su `TestClient` y solo recurre a `httpx` si `httpx2` falta. Son módulos distintos (`httpx` y `httpx2`), así que no hay conflicto. Se comprobó con `pip install --dry-run` de ambos archivos juntos, `pip check` y la suite del API con `-W error::DeprecationWarning`.

## Decisión 1 — Modelo meteorológico

Climatología mensual aproximada de GDL (Tmax, Tmin, punto de rocío, probabilidad diaria de lluvia y nubosidad media), interpolada por día. Conviene contrastarla con las normales del SMN si se requiere rigor. La hora local usa un offset fijo, `TZ_OFFSET_H=-6`: México no usa horario de verano desde 2022, así que no se necesita `zoneinfo` ni `tzdata`.

| Variable | Resolución | Rango simulado (GDL) | Validación del API | Modelo |
|---|---|---|---|---|
| `air_temperature_c` | 0.1 °C | ~5–35 | −40 a 60 | Media estacional + anomalía día a día + ciclo diurno asimétrico (mínimo al amanecer + 0.15 h, máximo al mediodía solar + 2.2 h) + ruido AR(1) − enfriamiento por lluvia (3–6 °C). La amplitud baja hasta 45 % con nubes |
| Punto de rocío (interno) | — | ~0–17 | no se envía | AR(1) lento (τ = 12 h). La lluvia lo acerca a T; siempre ≤ T. Solo sirve para calcular la HR |
| `relative_humidity_pct` | 1 % (entero) | ~15–100 | 0–100 | Fórmula de Magnus a partir de T y del punto de rocío; ≥ 90 % con lluvia |
| `station_pressure_hpa` | 0.1 hPa | ~836–846 | 500–1100 | Fórmula barométrica (840.4 hPa a 1,550 m) + marea semidiurna ±1.2 hPa (máximos a las 10 y 22 h) + deriva sinóptica AR(1) (τ = 2 días, acotada a ±3.5 hPa) |
| `wind_speed_ms` | 0.1 m/s | 0–10 | 0–75 | Weibull (k = 2) con autocorrelación: AR(1) gaussiano transformado por cuantil. Media de 1.0 m/s (~04 h) a 4.2 m/s (~16 h) |
| `wind_gust_ms` | 0.1 m/s | 0–20 | 0–100, ≥ velocidad | Velocidad × (1.3 + 0.1·log10(dt/10) + U(0, 0.25)), +0.4 al inicio de una tormenta |
| `wind_direction_deg` | 1° (entero) | 0–359 | 0–359 o null | Dirección predominante (`PREVAILING_WIND_DEG`, 250°) + desviación AR(1) lenta + ruido AR(1); `null` si la velocidad redondeada es < 0.5 m/s |
| `precipitation_mm` | 0.2 mm | 0 a ~10 por intervalo | 0–200 | Cadena de Markov en tiempo continuo. Inicios con tasa de Poisson, 75 % entre 16 y 22 h. Duración media 1.5 h. Intensidad lognormal (mediana 3 mm/h, tope 60 mm/h). Pluviómetro de balancín |
| `solar_radiation_wm2` | 1 W/m² (entero) | 0–~1,130 | 0–1500 | Cielo despejado (Haurwitz + 6 %/km de altitud) × (1 − 0.75·nubes^3.4), × 0.3 con lluvia; 0 con el sol bajo el horizonte |
| `device.battery_v` | 0.01 V | 12.0–13.8 | 0–15 | Carga solar hacia 13.8 V y descarga hacia 12.0 V, con integración exacta en dt |

**Coherencia con cualquier `INTERVAL_S` (10–3600 s):**

- Cada proceso AR(1) usa `phi = exp(-dt/tau)` con innovación `sigma·sqrt(1 − phi²)`. La varianza marginal y la autocorrelación en tiempo físico no dependen del intervalo; hay una prueba para dt = 10, 60, 300 y 3600.
- Los inicios y finales de lluvia usan probabilidades `1 − exp(−tasa·dt)`.
- **Precipitación por intervalo:** el balancín acumula `tasa·dt` y reporta las vueltas completas de 0.2 mm de cada intervalo. El resto queda para el siguiente. Una lluvia constante de 7.3 mm/h durante 3 h suma lo mismo, con diferencia menor a 0.2 mm, a 10, 60, 300 y 3600 s.
- **Inercia térmica:** la temperatura responde a la nubosidad y a la anomalía filtradas (paso bajo, τ = 3 h). Esto se añadió tras la primera medición: con la nubosidad instantánea, un claro a media tarde movía Tmax hasta las 17.5 h en ~10 % de los días secos.

**Redondeo sin artefactos de flotante:** primero se cuenta un entero (vueltas del balancín) y luego se aplica `round(vueltas × 0.2, 1)`. `json.dumps(3*0.2)` da `0.6000000000000001`; el generador da `0.6`. Además, `+ 0.0` convierte `-0.0` en `0.0`. Los campos enteros (`interval_s`, `sequence`, `wind_direction_deg`, HR, radiación) son `int` de Python y se serializan sin `.0`.

**`sequence`** empieza en 0 y sube en cada lectura generada, también en las que luego se mutan con una falla. Los huecos en el JSONL del API son exactamente las fallas.

**Timestamps:** son múltiplos del intervalo (piso del reloj de pared), en formato RFC 3339 UTC con `Z` a segundos, y nunca quedan en el futuro.

## Decisión 2 — Semillas y reproducibilidad

- `SEED` por defecto = los primeros 8 bytes de `sha256(STATION_ID)` como entero. No se usa `hash()`, que cambia en cada proceso. Así, cada estación tiene su serie y es estable entre ejecuciones y plataformas.
- El clima usa un `random.Random(SEED)` propio. Las fallas usan **otro** `random.Random`, sembrado con `sha256("faults:{SEED}")`. Cambiar `FAULT_RATE` no altera los valores meteorológicos de las lecturas que no se mutan (probado comparando el JSON byte a byte con 0 y 0.3).
- Misma semilla, mismo intervalo y mismo instante de arranque → la misma serie byte a byte (probado con sha256 del flujo de payloads).
- **Ojo:** con la misma `SEED` explícita en dos estaciones, las series son idénticas. Es lo esperado de una semilla reproducible.
- **Límite:** el resultado depende de `math` (sin, exp…) en el último bit. En otra plataforma o versión de Python, un valor podría cambiar en el redondeo. No se verificó fuera de Windows con Python 3.13.5.

## Decisión 3 — Política de respuestas, reintentos y búfer

| Respuesta | Clasificación | Acción |
|---|---|---|
| 2xx (201) | entregada (`created`) | Sale del búfer |
| 409 | entregada (`duplicates`) | Sale del búfer; ya estaba guardada, p. ej. si se perdió la respuesta del primer intento |
| 5xx, 408, 429, timeout, error de conexión o de protocolo | reintentable | Se queda al frente del búfer. Backoff `min(60, 2^(n−1)) × U(0.5, 1)` s; se reinicia tras cualquier resultado definitivo |
| 400, 413, 415, 422 y otros 4xx | no reintentable | Se descarta y se cuenta en `dropped_by_status`. Log WARNING con `status`, `problem_type` y los pares `[field, code]` del problem+json (sin `value` ni payload) |
| 401 | no reintentable | Se descarta. **ERROR** con `reason: check_station_key`: "el API no reconoce STATION_KEY; revisa la clave (y que su hash esté en STATION_KEY_HASHES del API)" |
| 403 | no reintentable | Se descarta. **ERROR** con `reason: check_station_id`: "la clave pertenece a otra estación; revisa STATION_ID y STATION_KEY" |

**401/403 no detienen la estación:** cada lectura nueva hace un solo intento y se descarta. Así queda un ERROR por intervalo, sin bucle de reintentos. Si la configuración del API se corrige, las lecturas siguientes se aceptan sin reiniciar la estación.

**Búfer:**
- En memoria, FIFO, acotado a `BUFFER_MAX`.
- Solo se envía el frente, así que el orden de entrega se conserva.
- Si se llena, se descarta la lectura **más vieja**, con contador (`dropped_overflow`) y WARNING (`buffer_overflow`).

**Programación de ticks:**
- **Modo vivo:** se programa sobre el reloj monotónico desde el arranque (`instante = inicio + (ts − arranque)`), no con `sleep(intervalo)` tras cada envío. No hay deriva acumulada aunque las solicitudes tarden (probado con solicitudes de 3.7 s: 60 lecturas exactas por hora). Los ticks atrasados se generan todos, cada uno con su timestamp.
- **Salto del reloj de pared hacia adelante (solo modo vivo):** se calcula la deriva `pared − (pared_ancla + (monotónico − monotónico_ancla))`.
  - **Motivo:** si el equipo se suspende, el reloj de pared avanza y el monotónico puede no hacerlo. Sin corrección, la serie quedaba atrasada para siempre: con un salto de +8 h, el último timestamp iba 8.01 h detrás del reloj de pared y seguía separado 60 s.
  - **Reanclaje:** si la deriva supera `max(2 × INTERVAL_S, 5 s)`, el calendario se reancla al instante actual. El siguiente timestamp es el piso del reloj de pared de ahora, siempre estrictamente mayor que el último generado.
  - **Ticks perdidos:** se omiten, no se rellenan, porque una estación suspendida no mide. `sequence` sigue contigua, pues solo cuenta lecturas generadas. Los huecos de timestamp en el JSONL son el tiempo suspendido.
  - **Aviso:** se registra un WARNING `clock_jump` con `jump_s` (deriva) y `ticks_skipped` (timestamps alineados que no se generaron).
  - **Saltos pequeños:** uno menor que el umbral no se reancla y la serie sigue contigua, con ese atraso.
- **Salto del reloj de pared hacia atrás:** no se corrige. El modelo exige tiempo creciente, y reanclar repetiría timestamps ya guardados, que darían 409. La estación espera a que el reloj de pared alcance el siguiente timestamp; no genera nada mientras tanto y nunca envía un timestamp en el futuro. Con −10 min, el tick siguiente esperó esos 10 min.
- **Espera:** usa `Event.wait` en tramos de 0.5 s para atender señales pronto, también en Windows.

**Fin:**
- Con `MAX_READINGS`, la estación termina cuando entregó o descartó todo lo generado.
- Con SIGINT, SIGTERM o SIGBREAK, se activa un `Event`, se intenta vaciar el búfer durante 10 s como máximo y se registra lo que quede (`unsent_at_exit`).
- En ambos casos imprime un resumen JSON (`event: summary`) y sale con código 0.
- El manejador de señal no escribe logs: no es seguro dentro de un manejador.

## Decisión 4 — Modo réplica

- Con `REPLAY_HOURS = N > 0`, al arrancar se generan las lecturas de las últimas N horas: `N·3600 // INTERVAL_S` timestamps alineados, el último igual al piso del arranque, todos ≤ ahora.
- Se envían repartidas en `REPLAY_DURATION_MIN` (0 = lo más rápido posible).
- Después sigue el modo vivo con el mismo modelo y la misma `sequence`, sin hueco ni repetición de timestamps.
- `REPLAY_HOURS` tiene un máximo de 167: el API rechaza timestamps de más de 7 días (168 h) y queda 1 h de margen para enviar.
- **Contrapresión:** la siguiente lectura de réplica se genera solo cuando el búfer está vacío. Las lecturas en vivo no esperan, porque dependen del reloj. Si el API cae a mitad de una réplica, la réplica se pausa en lugar de desbordar el búfer.
- **Salto del reloj durante la réplica:** no se reancla. La réplica conserva sus timestamps, todos fijados al arrancar y ≤ el arranque. El salto se detecta y se reancla en el primer tick vivo, con el mismo WARNING `clock_jump`. Así, el modo vivo empieza en el piso del reloj de pared de ese momento, y no en arranque + intervalo con horas de atraso.

## Decisión 5 — Catálogo de fallas

Con probabilidad `FAULT_RATE` por lectura se aplica **una** mutación sobre una copia del payload ya generado. Las 9 dan 422 con exactamente un error:

| Falla | Mutación | `field` esperado | `code` esperado |
|---|---|---|---|
| `humidity_over_100` | HR = 101 | `measurements.relative_humidity_pct` | `less_than_equal` |
| `temperature_below_min` | T = −41 | `measurements.air_temperature_c` | `greater_than_equal` |
| `gust_below_speed` | velocidad = máx(velocidad, 1); ráfaga = velocidad − 0.5 | `measurements.wind_gust_ms` | `gust_less_than_speed` |
| `extra_field` | `"unexpected_field": 1` en la raíz | `unexpected_field` | `extra_forbidden` |
| `missing_required` | sin `station_pressure_hpa` | `measurements.station_pressure_hpa` | `missing` |
| `string_in_number` | T como texto (`"24.7"`) | `measurements.air_temperature_c` | `float_type` |
| `future_timestamp` | **reloj actual** + 10 min | `timestamp` | `timestamp_in_future` |
| `bad_schema_version` | `"2.0"` | `schema_version` | `literal_error` |
| `float_in_integer` | `interval_s` como flotante (60 → 60.0) | `interval_s` | `int_type` |

**Comprobación:** el emisor compara la respuesta real con lo esperado. Si el 422 trae exactamente ese par (field, code), registra `fault_confirmed` (INFO). Si no (otro código, otro status o un 201), registra `fault_mismatch` (WARNING) con `expected` y `received`. Los contadores van al resumen: `faults_injected`, `faults_confirmed` y `faults_unexpected`.

**Ajuste respecto al planteamiento:**
- `future_timestamp` usa **reloj actual + 10 min**, no timestamp de la lectura + 10 min. En réplica, o con intervalos largos (p. ej. 3600 s), el timestamp + 10 min sigue en el pasado y el API lo aceptaría.
- `float_in_integer` usa `float(INTERVAL_S)` y no siempre `60.0`, para no cambiar el intervalo de la lectura.

## Seguridad de la clave

- `STATION_KEY` se guarda en un `Secret` cuyo `repr`, `str` y f-string muestran `***`. El dataclass `Config` la excluye de su `repr`.
- Los errores de configuración no reflejan **ningún** valor, por si alguien pega la clave en otra variable.
- Los logs nunca llevan el payload completo ni los `value` del problem+json.
- Las excepciones de transporte se registran solo por su tipo.

## Verificación (resumen; evidencia completa en el reporte de la fase)

- **Pruebas:** station 155 (151 + 4 del salto de reloj), ingest_api 98, contract_tests 12. El contrato valida 4,320 payloads (24 h × 3 estaciones) contra el `WeatherReading` real, y cada falla devuelve exactamente su (field, code).
- **Procesos reales, sin Docker:** se comprobaron estos puntos contra uvicorn real.
  - 3 estaciones en vivo (18 líneas únicas).
  - Réplica de 2 h con repetición: 409 contados como `duplicates`; líneas nuevas = timestamps nuevos.
  - Fallas con `FAULT_RATE=0.3`: todas confirmadas y 0 `fault_mismatch`; los huecos coinciden con las fallas.
  - Caída del API de 50 s: entrega en orden, sin pérdidas ni duplicados.
  - 401 y 403 sin reintentos.
  - Cierre con `CTRL_BREAK_EVENT`.
  - Búsqueda de las 3 claves en claro: 0 coincidencias.
- **Bug hallado y corregido durante la verificación:**
  - **Síntoma:** una réplica de 24 h con `REPLAY_DURATION_MIN=0` envió 549 de 1,440 lecturas.
  - **Causa:** el bucle generaba todas las lecturas vencidas antes de enviar, así que desbordó `BUFFER_MAX=1000` y descartó las 440 más viejas. Además, `MAX_READINGS` cortaba al generar la última, y el vaciado de 10 s no alcanzó.
  - **Corrección:** contrapresión en la réplica, y `MAX_READINGS` termina al entregar.
  - **Pruebas de regresión:** 3 en `test_main.py`.

## Límites conocidos y lo que no se verificó

- **Suspensión real del equipo:** no se verificó una suspensión real del equipo ni de Docker Desktop. El salto del reloj de pared solo se simuló con el reloj falso de las pruebas (+8 h, +100 s, −10 min y +8 h durante una réplica). Tampoco se midió si en Windows `time.monotonic()` avanza o no durante la suspensión. Si avanzara, no habría deriva y la estación generaría de golpe los ticks atrasados, como en cualquier atraso.
- **SIGINT real en Windows:** se demostró el cierre ordenado con `CTRL_BREAK_EVENT` (SIGBREAK) sobre un proceso real en su propio grupo de consola. SIGINT (`CTRL_C_EVENT`) no se envió a un proceso real: en Windows, `CTRL_C_EVENT` llega a todo el grupo de consola, incluida la terminal que lo envía. SIGINT sí se probó en el mismo proceso con `signal.raise_signal`. SIGTERM no se probó en un proceso real: en Windows, `Popen.terminate()` es `TerminateProcess`, que no entrega ninguna señal.
- **El búfer vive en memoria:** si el proceso muere (kill, corte de luz), se pierde lo pendiente. El vaciado de 10 s solo ocurre con un cierre ordenado.
- **401/403:** cada lectura afectada se pierde (se descarta por diseño) hasta corregir la clave o el `station_id`.
- **`future_timestamp` puede no dar 422:** pasa si la lectura espera en el búfer más de 5 min (caída del API) o si el API tiene `MAX_FUTURE_SKEW_S` ≥ 600. Si el API la aceptara, ocuparía un timestamp futuro, y la lectura real de ese instante recibiría 409. El emisor lo registraría como `fault_mismatch`. No ocurrió en la verificación.
- **No se honra `Retry-After`** en 429/503; el API actual no lo envía.
- **Rangos simulados aproximados:** en la prueba de 20 semillas, la mínima simulada en enero llegó a 4.8 °C, ligeramente bajo el "~5" de la tabla. La radiación máxima de cielo despejado en enero es ~830 W/m² (sol más bajo), así que el rango 900–1,200 se cumple en abril y octubre, las fechas de prueba, no todo el año. En julio, con 13–15 de 20 días con lluvia, hay pocos días secos para medir Tmax y Tmin; esas propiedades se prueban en abril y octubre.
- **Velocidad media con intervalos largos:** con `INTERVAL_S=3600`, la velocidad reportada es un valor del proceso en ese instante, no un promedio de la hora. Solo la ráfaga crece con el intervalo.
- **Rendimiento de las pruebas:** `test_weather_model.py` simula 415,000 pasos (20 semillas × 2 fechas × 3 intervalos). En esta máquina, con el CPU limitado por el sistema durante la sesión (un bucle de referencia tardó entre 0.38 y 5.9 s), la suite completa tardó 11 min 35 s; sin esa limitación tardó 67 s. Casi todo el tiempo es la corrida a 10 s.
- **Fuera de alcance de esta fase:** Docker y compose (Fase C), diagramas C4 (Fase D) y Azure.
