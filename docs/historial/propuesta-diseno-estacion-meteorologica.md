# Investigación y propuesta de diseño — Estación meteorológica simulada (Docker) y base para el C4

**Proyecto:** Skilliket — TEC de Monterrey GDA
**Fecha:** 2026-10-01
**Estado:** Propuesta (sin implementar)

---

## 1. Estado actual del repositorio y del entorno

**No existe nada de Skilliket todavía.**

| Verificación | Resultado |
|---|---|
| `C:\Users\orizo\OneDrive\Documentos\TEC\SKILLIKET` | Existía pero estaba **vacía (0 elementos, incluidos ocultos)** antes de crear este documento |
| Repositorio git | **No** (no hay `.git`) |
| Código, Dockerfile, compose, CI, docs | **Ninguno** |
| Configuración de Azure (`~\.azure`) | **No existe** |
| Azure CLI (`az`) | **No instalada** |
| Docker | CLI 29.6.1 instalado; `docker info` no respondió → **el daemon (Docker Desktop) parece estar apagado** |
| Python local | 3.14.3 |
| Git | Instalado |

No se revisaron las carpetas hermanas (`PRIMER/SEGUNDO/TERCER SEMESTRE`).

---

## 2. Ingesta: IoT Hub vs. API REST propia

| Criterio | (a) Dispositivo → **IoT Hub** | (b) Dispositivo → **API propia en Container Apps** |
|---|---|---|
| **Autenticación** | Identidad por dispositivo en el *identity registry*: token SAS firmado con HMAC-SHA256 usando la clave simétrica del dispositivo (*"the signing keys… are never sent over the wire"*) o X.509 (Microsoft recomienda X.509 firmado por CA para producción; el autofirmado se registra por huella). | Sin identidad de dispositivo integrada; se implementa una API key por estación. *Easy Auth* existe, pero está orientado a proveedores de identidad (Entra, Google…) y flujos de usuario; para dispositivos requeriría client credentials de Entra (JWT), más complejo. La ingress también admite **certificados de cliente (mTLS)**. |
| **Protocolo desde el contenedor** | MQTT (8883), AMQP (5671), sus variantes por WebSockets (443) o HTTPS (443) con el SAS en `Authorization` | HTTPS; TLS 1.2/1.3 terminado en la ingress |
| **Nivel gratis** | **Free**: 8,000 mensajes/día, medidor de 0.5 KB, 500 dispositivos, **1 hub gratuito por suscripción**. Un payload de ~400 B cuenta como 1 mensaje → a 1 lectura/min caben **5 estaciones como máximo** (1,440 msg/día cada una). | **Consumption**: gratis 180,000 vCPU-s, 360,000 GiB-s y 2 M de solicitudes al mes por suscripción; con 0 réplicas no se cobra. Cálculo propio: una réplica de 0.25 vCPU/0.5 GiB encendida todo el mes consume 648,000 vCPU-s, **≈3.6× la franja gratis**; cabe si funciona ~8 días al mes. Esa réplica se cobra a tarifa activa (la tarifa *idle* solo aplica con `minReplicas > 0`). |
| **Validación de esquema** | **IoT Hub no valida el cuerpo**; solo enruta (endpoint integrado compatible con Event Hubs, Storage, Service Bus, Cosmos DB). Hace falta *además* un consumidor que valide, sin poder responder 4xx al dispositivo. | La validación ocurre **en la misma solicitud**; el dispositivo recibe 202/4xx en ese momento |
| **Complejidad para un prototipo** | Media–alta: hub, registro de dispositivos, SDK o generación de SAS, consumidor del endpoint compatible con Event Hubs y dónde corre ese consumidor | Baja: la misma imagen Docker local se despliega tal cual |
| **Encaje con "endpoint REST que recibe y valida JSON"** | Indirecto: el endpoint REST sería del propio IoT Hub y no valida | **Directo**: es literalmente el entregable |
| **Qué se vería en el C4** | Estación → IoT Hub (SAS/X.509) → ruta → endpoint Event Hub → Validator (Function/Container App) → Storage | Estación → Ingest API (HTTPS + API key) → Storage, más el almacén de secretos con identidad administrada |

### Recomendación: (b) Azure Container Apps

- El entregable pide un endpoint REST que valide; en (a) ese endpoint sería solo un consumidor extra que nunca responde al dispositivo.
- Container Apps es preferible a App Service: App Service F1 ofrece 60 min de CPU/día en VMs compartidas y *"Use of free plan for production workloads is not supported"*; Container Apps tiene escalado a cero y health probes.
- IoT Hub queda documentado en el C4 como **ruta de evolución** (identidad por dispositivo, X.509).

---

## 3. Framework del endpoint

| Criterio | **FastAPI + Pydantic v2** | Flask + jsonschema | Azure Functions (Python) |
|---|---|---|---|
| Validación de esquema | Tipos, rangos `ge/le`, regex, `extra="forbid"`, validadores cruzados (`model_validator`), errores con ruta (`loc`) | JSON Schema estándar; las reglas cruzadas (ráfaga ≥ velocidad, punto de rocío ≤ T) requieren código aparte | Manual o con Pydantic |
| Tamaño de imagen Docker | Base `python:3.13-slim` = 43.0 MB comprimida (amd64, Docker Hub); las dependencias difieren en pocos MB entre opciones (se medirá en la Fase 4) | Igual | Requiere la imagen del host de Functions (más pesada) y otro modelo de programación |
| Documentación automática | OpenAPI + Swagger UI incluidos (`/docs`) | Requiere flasgger/apispec | Parcial |
| Despliegue en Azure | Cualquier contenedor en Container Apps | Igual | Nativo, pero se aleja de "un contenedor Docker" |
| Testabilidad | `TestClient` + `dependency_overrides` para sustituir auth y storage | `test_client`, también bueno | Más fricción |

### Recomendación: FastAPI + Pydantic v2

Versiones actuales en PyPI: fastapi 0.142.2, pydantic 2.13.5.

**Razón concreta:** un solo modelo Pydantic valida la solicitud, exporta el JSON Schema (`model_json_schema()`) y alimenta OpenAPI. El esquema documentado en el C4 y el validador no pueden desincronizarse.

**Detalle a manejar:** según su documentación, FastAPI responde **422** por defecto ante cualquier error de validación, incluido el JSON mal formado. Hay que sobrescribir el handler de `RequestValidationError` para distinguir 400 de 422 (ver 5c).

---

## 4. Variables meteorológicas a simular

Estación de referencia: **Guadalajara (~1,550 m)**.

- **Rango simulado:** climatología aproximada de GDL (conviene contrastar con datos del SMN/CONAGUA si se requiere rigor).
- **Rango de validación:** límites físicos/del sensor; el API rechaza lo que quede fuera.

| Variable | Unidad / resolución | Rango simulado (GDL) | Rango de validación | Modelo y correlaciones |
|---|---|---|---|---|
| Temperatura del aire | °C / 0.1 | ~5–36 | −40 a 60 | Media estacional + ciclo diurno asimétrico: mínimo cerca del amanecer (~07 h), máximo ~15 h. Amplitud ~16 °C en época seca y ~10 °C en lluvias o días nublados. Ruido AR(1), no uniforme |
| Punto de rocío | °C / 0.1 | ~0–17 | −60 a T | Variable lenta (AR(1)): seca ~2–8 °C, lluvias ~14–16 °C. **Siempre ≤ T** |
| Humedad relativa | % / 1 | ~15–100 | 0–100 | **Se calcula** con la fórmula de Magnus a partir de T y punto de rocío → anticorrelacionada con T en el ciclo diurno; ~95–100 % con lluvia |
| Presión de estación | hPa / 0.1 | ~835–848 | 500–1100 | Base ~840 hPa (fórmula barométrica a 1,550 m) + marea semidiurna ±1–1.5 hPa (máximos ~10 h y ~22 h) + deriva sinóptica lenta ±3 hPa |
| Velocidad del viento | m/s / 0.1 | 0–10 | 0–75 | Base Weibull; mínima al amanecer, máxima 14–17 h. < 0.5 m/s = calma |
| Ráfaga | m/s / 0.1 | 0–20 | 0–100 | Velocidad × 1.3–1.8, **≥ velocidad**; picos al inicio de la lluvia |
| Dirección del viento | ° / 1 | 0–359 | 0–359 o `null` | Dirección predominante configurable + caminata aleatoria; `null` en calma |
| Precipitación del intervalo | mm / **0.2** (pluviómetro de balancín) | 0–~10 por intervalo | 0–200 | Eventos de Markov: más probables en temporada de lluvias por la tarde-noche (16–22 h). Con lluvia: T baja 3–6 °C, HR sube, radiación cae |
| Radiación solar | W/m² / 1 | 0–~1,100 | 0–1,500 | Cielo despejado según elevación solar (lat 20.67, lon −103.35) × factor de nubosidad; **0 de noche** |
| Batería (opcional) | V / 0.01 | 12.0–13.8 | 0–15 | Sube de día (carga solar), baja de noche |

**Extras del generador:**

- Semilla reproducible.
- Redondeo a la resolución del sensor.
- **Modo réplica:** comprime las últimas 24 h en N minutos con timestamps ≤ ahora (para demos sin violar la ventana de tiempo).
- **Inyección de fallas** opcional (probabilidad configurable, 0 por defecto) para demostrar los 422.

---

## 5. Diseño propuesto

### a) Esquema del payload JSON (v1)

```json
{
  "schema_version": "1.0",
  "station_id": "GDL-001",
  "timestamp": "2026-10-01T21:15:00Z",
  "sequence": 1834,
  "interval_s": 60,
  "measurements": {
    "air_temperature_c": 24.7,
    "relative_humidity_pct": 58,
    "dew_point_c": 15.9,
    "station_pressure_hpa": 841.3,
    "wind_speed_ms": 3.4,
    "wind_gust_ms": 5.1,
    "wind_direction_deg": 245,
    "precipitation_mm": 0.0,
    "solar_radiation_wm2": 612
  },
  "device": { "battery_v": 13.21, "firmware": "sim-0.1.0" }
}
```

| Campo | Tipo | Obligatorio | Regla |
|---|---|---|---|
| `schema_version` | string | Sí | Constante `"1.0"` |
| `station_id` | string | Sí | Patrón `^[A-Z]{3}-\d{3}$` |
| `timestamp` | string (RFC 3339) | Sí | UTC con `Z`; ≤ 5 min en el futuro y ≤ 7 días en el pasado (configurables) |
| `sequence` | integer | No | ≥ 0; permite detectar huecos |
| `interval_s` | integer | Sí | 10–3600 |
| `measurements.air_temperature_c` | number | Sí | −40 a 60 |
| `measurements.relative_humidity_pct` | number | Sí | 0–100 |
| `measurements.dew_point_c` | number | No | −60 a `air_temperature_c` |
| `measurements.station_pressure_hpa` | number | Sí | 500–1100 |
| `measurements.wind_speed_ms` | number | Sí | 0–75 |
| `measurements.wind_gust_ms` | number | No | 0–100 y ≥ `wind_speed_ms` |
| `measurements.wind_direction_deg` | integer \| null | No | 0–359; `null` si velocidad < 0.5 |
| `measurements.precipitation_mm` | number | Sí | 0–200 |
| `measurements.solar_radiation_wm2` | number | No | 0–1500 |
| `device.battery_v` | number | No | 0–15 |
| `device.firmware` | string | No | — |

- `extra="forbid"` en todos los objetos: se rechazan campos desconocidos.

### b) Estructura de archivos, Dockerfile y docker-compose

**Decisión: dos contenedores.** Son responsabilidades con ciclos de vida distintos; en Azure solo se despliega el API y el generador apunta a él cambiando `INGEST_URL`. La documentación de Container Apps recomienda un servicio por app.

```
skilliket-weather-station/
├── README.md
├── docker-compose.yml
├── .env.example
├── .gitignore
├── .dockerignore
├── schema/
│   └── weather_reading.v1.schema.json      # exportado desde Pydantic
├── station/                                 # emulador de estación
│   ├── Dockerfile
│   ├── requirements.txt                     # solo httpx
│   ├── station/__init__.py
│   ├── station/config.py                    # env: STATION_ID, INGEST_URL, INTERVAL_S, SEED, REPLAY_HOURS, FAULT_RATE
│   ├── station/weather_model.py             # modelo físico (sección 4)
│   ├── station/sender.py                    # POST con reintentos/backoff y búfer local
│   ├── station/main.py
│   └── tests/test_weather_model.py
├── ingest_api/
│   ├── Dockerfile
│   ├── requirements.txt                     # fastapi, uvicorn
│   ├── app/__init__.py
│   ├── app/main.py                          # rutas + handlers
│   ├── app/models.py                        # Pydantic WeatherReading v1
│   ├── app/auth.py                          # API key por estación
│   ├── app/errors.py                        # application/problem+json
│   ├── app/storage.py                       # interfaz + implementación en memoria/log
│   ├── app/config.py
│   └── tests/
│       ├── test_readings.py
│       └── test_auth.py
├── tests/test_contract.py                   # salida del generador → modelo del API
└── docs/adr/0001-ingesta.md
```

**Dockerfile del API (boceto):**

- `FROM python:3.13-slim`
- Usuario no-root
- `pip install --no-cache-dir -r requirements.txt`
- `EXPOSE 8000`
- `HEALTHCHECK` con `python -c "urllib…"` (la imagen slim no trae curl)
- `CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]`

Se elige slim y no alpine (16.9 MB): alpine pesa menos, pero slim evita problemas con wheels musl.

**Dockerfile de la estación:** igual, sin puerto expuesto; `CMD ["python", "-m", "station.main"]`.

**docker-compose:**

- `ingest-api`: puerto 8000, healthcheck.
- `station-gdl-001` (y opcionalmente `station-gdl-002`): `depends_on: { ingest-api: { condition: service_healthy } }`, `INGEST_URL=http://ingest-api:8000/v1/readings`, claves desde `.env`.

### c) Comportamiento del endpoint `POST /v1/readings`

Comprobaciones en este orden; todos los errores usan **RFC 9457 `application/problem+json`**.

| Caso | Código | Cuerpo |
|---|---|---|
| Falta la clave o es inválida | **401** | `{type, title: "Unauthorized", status, detail}` |
| `Content-Type` distinto de `application/json` | **415** | problem |
| Cuerpo > 4 KB | **413** | problem |
| **JSON mal formado** (no se puede parsear) | **400** | problem con `detail` (línea/columna) |
| **JSON válido pero esquema inválido** (tipo, rango, campo faltante o extra, regla cruzada, timestamp fuera de ventana) | **422** | problem + `errors: [{field: "measurements.relative_humidity_pct", code: "less_than_equal", message, value}]` |
| `station_id` del cuerpo distinto del ligado a la clave | **403** | problem |
| Duplicado (`station_id` + `timestamp`), opcional | **409** | problem |
| **Válido** | **202** | `{status: "accepted", reading_id, station_id, received_at}` |
| Error interno | 500 | problem genérico, sin traza |

- Se usa 202 y no 201 porque la persistencia no está decidida; si se guarda de forma síncrona, pasa a 201.
- Rutas adicionales: `GET /health` (sin autenticación, para las probes) y `/docs` + `/openapi.json`.

### d) Autenticación de dispositivos (opción b)

- La estación envía la cabecera `X-Station-Key` por HTTPS (TLS en la ingress de Container Apps).
- El API guarda **solo hashes SHA-256** de cada clave, ligados a un `station_id`, y compara con `hmac.compare_digest`.
- **Dos claves por estación** para rotar sin cortes (patrón primaria/secundaria de IoT Hub).
- Los hashes viven como secreto de Container Apps o como **referencia a Key Vault con identidad administrada** (rol *Key Vault Secrets User*); la documentación desaconseja valores directos en producción.
- El API escribe en storage con **identidad administrada**, sin cadenas de conexión.
- Evolución documentada en el C4: JWT de Entra (client credentials) o mTLS.

### e) Elementos preliminares del C4

**Nivel 1 — Contexto**

- Persona: *Administrador de Skilliket* (registra estaciones y claves).
- Sistema: *Plataforma de ingesta meteorológica Skilliket (Azure)*.
- Sistemas externos: *Estación meteorológica (simulada en Docker)*, *Microsoft Entra ID* (identidades administradas).
- Relación principal: estación → plataforma, "envía lecturas JSON/HTTPS + API key".

**Nivel 2 — Contenedor**

- Dentro del límite del sistema: *Ingest API* (FastAPI, Container App), *Almacén de lecturas* (según decisión), *Key Vault / secretos*, *Log Analytics*, *Registro de contenedores* (en tiempo de despliegue).
- Fuera del límite: *Station Simulator* (contenedor Docker).
- Puntos de autenticación:
  1. Estación → API: TLS + API key
  2. API → storage: identidad administrada
  3. API → Key Vault: identidad administrada
  4. Container App → registro: identidad administrada (`acrPull`)

**Nivel 3 — Componente (Ingest API)**

- Router HTTP
- Verificador de API key
- Guardia de solicitud (tipo y tamaño)
- Modelo `WeatherReading` (Pydantic)
- Mapeador de errores problem+json
- Servicio de lecturas (id, `received_at`, deduplicación)
- Adaptador de storage
- Configuración y logging

**Nivel 3 — Componente (Station Simulator, opcional)**

- Reloj (real o réplica)
- Modelo meteorológico
- Constructor de payload
- Emisor con reintentos y búfer

### f) Fases y criterios de verificación

| Fase | Contenido | Verificación |
|---|---|---|
| 0 | Decisiones abiertas y ADR de ingesta | ADR escrito con la opción elegida |
| 1 | Modelo Pydantic, JSON Schema exportado y fixtures válidos e inválidos | `pytest`: todos los válidos pasan; cada inválido falla con el `code`/`field` esperado |
| 2 | Generador (sin Docker) | Tests de propiedades sobre 24 h simuladas: HR en [0, 100], punto de rocío ≤ T, ráfaga ≥ velocidad, Tmax entre 13 y 17 h, radiación 0 de noche, corr(T, HR) < −0.5, determinista con semilla, 100 % de payloads válidos con `FAULT_RATE=0` |
| 3 | Ingest API | `TestClient` cubre 202/400/401/403/413/415/422 con el cuerpo esperado; `/docs` carga |
| 4 | Docker y compose | `docker compose up` deja los servicios *healthy*; el log del API muestra 202 por lectura; contenedores no-root; tamaño de imagen registrado; test de contrato pasa |
| 5 | Despliegue en Azure (si se decide) | `curl` al FQDN: sin clave → 401, con clave → 202; logs visibles en Log Analytics; consumo dentro del presupuesto |
| 6 | Diagramas C4 (3 niveles) | Cada elemento corresponde a un archivo o recurso real; los 4 puntos de autenticación anotados |

---

## 6. Decisiones pendientes

1. **Ingesta:** ¿se confirma (b) Container Apps, o se prefiere IoT Hub (o un esquema híbrido) por razones académicas o de rúbrica?
2. **Qué se despliega en Azure:**
   - (i) nada, solo Docker local;
   - (ii) el API en Container Apps y el generador local;
   - (iii) ambos, con el generador como Container Apps Job.
3. **Costo y suscripción:** ¿se usará Azure for Students (USD 100 por 12 meses, sin tarjeta)? ¿Quién es dueño de la suscripción? Con el API encendido 24/7 se excede la franja gratis (~3.6×). Propuesta: escalar a cero y hacer sesiones de demo, o enviar cada 5 min. ¿Se configura una alerta de presupuesto?
4. **Persistencia:** ninguna (solo log), Table Storage, Blob (JSON Lines) o Cosmos DB en nivel gratuito. Cambia el diagrama de contenedores. Precios de Storage no verificados.
5. **Credenciales y repositorio:** la carpeta no es repo git y no hay CI. ¿Dónde vivirá el repo (GitHub)? ¿Qué registro de imágenes: ACR (de pago, precio no verificado) o GHCR/Docker Hub (Docker Hub tiene límites de descarga, según Microsoft)? Hay que instalar `az` antes de la Fase 5.
6. **Estaciones:** cuántas, sus IDs, coordenadas reales (¿campus GDA?) e intervalo de envío.
7. **Herramienta para el C4:** Structurizr DSL, Mermaid C4, PlantUML o draw.io.
8. **Docker:** el daemon no respondió; encender Docker Desktop antes de la Fase 4.

---

## Fuentes

- [Azure IoT Hub pricing](https://azure.microsoft.com/en-us/pricing/details/iot-hub/)
- [IoT Hub — Control access with shared access signatures](https://learn.microsoft.com/en-us/azure/iot-hub/authenticate-authorize-sas)
- [IoT Hub — Authenticate with X.509 certificates](https://learn.microsoft.com/en-us/azure/iot-hub/authenticate-authorize-x509)
- [IoT Hub — Quotas and throttling](https://learn.microsoft.com/en-us/azure/iot-hub/iot-hub-devguide-quotas-throttling)
- [IoT Hub — Message routing](https://learn.microsoft.com/en-us/azure/iot-hub/iot-hub-devguide-messages-d2c)
- [IoT Hub — Communication protocols and ports](https://learn.microsoft.com/en-us/azure/iot-hub/iot-hub-devguide-protocols)
- [Azure Container Apps pricing](https://azure.microsoft.com/en-us/pricing/details/container-apps/)
- [Container Apps — Billing](https://learn.microsoft.com/en-us/azure/container-apps/billing)
- [Container Apps — Authentication and authorization](https://learn.microsoft.com/en-us/azure/container-apps/authentication)
- [Container Apps — Ingress](https://learn.microsoft.com/en-us/azure/container-apps/ingress-overview)
- [Container Apps — Manage secrets](https://learn.microsoft.com/en-us/azure/container-apps/manage-secrets)
- [Container Apps — Containers](https://learn.microsoft.com/en-us/azure/container-apps/containers)
- [App Service on Linux pricing](https://azure.microsoft.com/en-us/pricing/details/app-service/linux/)
- [App Service — Run a custom container](https://learn.microsoft.com/en-us/azure/app-service/quickstart-custom-container)
- [Azure for Students](https://azure.microsoft.com/en-us/free/students/)
- [FastAPI — Handling errors](https://fastapi.tiangolo.com/tutorial/handling-errors/)
- Docker Hub API (tags `python:3.13-slim` y `python:3.13-alpine`) y PyPI JSON API (versiones de paquetes), consultados el 2026-10-01
