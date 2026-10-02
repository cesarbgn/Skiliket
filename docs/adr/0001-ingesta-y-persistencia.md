# ADR 0001 — Ingesta y persistencia de lecturas meteorológicas

- **Estado:** Aceptada
- **Fecha:** 2026-10-01
- **Alcance:** Fase A (contrato y API de ingesta). Ejecución solo en Docker local; Azure es el destino que describe el C4.

## Contexto

Skilliket necesita un prototipo que emule una estación meteorológica en Docker, genere datos sintéticos y los envíe a un endpoint REST que reciba y valide payloads JSON. La infraestructura objetivo es Azure. Hubo que decidir cómo entran los datos (IoT Hub o API propia) y dónde se guardan.

## Decisión 1 — Ingesta: API REST propia (FastAPI + Pydantic v2)

La estación envía `POST /v1/readings` por HTTPS al **Ingest API**. El objetivo en Azure es **Azure Container Apps** (plan Consumption). En esta fase solo corre en local.

### Razones

- El entregable pide un endpoint REST que **valide** el JSON y responda al dispositivo. IoT Hub no valida el cuerpo de los mensajes: solo los enruta. Habría que añadir un consumidor aparte, que además no puede devolver un 4xx a la estación.
- La misma imagen Docker que corre en local se despliega sin cambios en Container Apps. La plataforma da TLS en la ingress, escalado a cero, health probes, secretos con referencias a Key Vault e identidad administrada.
- App Service F1 queda descartado: CPU compartida limitada a 60 min/día, y Microsoft no lo admite para producción.
- Con un solo modelo Pydantic se obtienen la validación, el JSON Schema publicado (`schema/weather_reading.v1.schema.json`) y OpenAPI, así que no pueden desincronizarse.

### Alternativa descartada y ruta de evolución: Azure IoT Hub

IoT Hub aporta una identidad por dispositivo (SAS con clave simétrica o X.509), MQTT/AMQP y enrutamiento a Storage, Event Hubs, Service Bus o Cosmos DB. Su nivel Free da 8,000 mensajes/día con medidor de 0.5 KB y un hub gratuito por suscripción. Se reconsiderará cuando:

- el número de estaciones o la frecuencia superen lo razonable para API keys gestionadas a mano,
- se necesiten comandos o configuración nube→dispositivo (device twins, C2D),
- o se exija autenticación por certificado X.509 con aprovisionamiento (DPS).

En ese escenario, la lógica de validación de este API se reutiliza como consumidor del endpoint compatible con Event Hubs.

## Decisión 2 — Autenticación de estaciones

- Cabecera `X-Station-Key` con una clave aleatoria de 32 bytes (`scripts/gen_station_key.py`).
- El servidor recibe **solo hashes SHA-256** por la variable de entorno `STATION_KEY_HASHES` (`{station_id: [hash1, hash2]}`). Cada estación tiene una o dos claves, para rotar sin cortes. Un hash no puede repetirse entre estaciones.
- La comparación usa `hmac.compare_digest` y recorre todas las entradas.
- Si el `station_id` del cuerpo no corresponde a la clave, la respuesta es 403.
- Las claves en claro no se escriben en archivos versionados ni en logs. Las pruebas generan sus claves al ejecutarse.
- **Objetivo en Azure:** hashes como secreto de Container Apps con referencia a Key Vault, leídos con identidad administrada (rol *Key Vault Secrets User*). Evolución posible: JWT de Entra ID (client credentials) o mTLS en la ingress.

## Decisión 3 — Persistencia

- Interfaz `StorageAdapter.save()`, **síncrona**. Una lectura solo se confirma después de escribirse.
- Implementación local `JsonlStorage`: añade una línea JSON por lectura validada a `STORAGE_PATH` (por defecto `./data/readings.jsonl`, ignorado por git). Cada línea es `{reading_id, received_at, reading}`, donde `reading` es el payload normalizado. La escritura ocurre bajo un lock, como una sola operación `write` de la línea completa, seguida de `flush` y `fsync`.
- **Destino en Azure (no implementado):** un adaptador para Azure Blob Storage (append blobs o un blob por lectura), autenticado con identidad administrada. La interfaz no cambia.

## Cambios respecto a la propuesta inicial y sus razones

1. **Se quita `dew_point_c` del payload.** Es un dato derivado de la temperatura y la HR. Si la estación lo enviara, habría dos fuentes de verdad que podrían contradecirse. Si llega, se rechaza como campo extra (`extra_forbidden`, 422).
2. **Éxito = `201 Created`** (no 202), porque la persistencia es síncrona: cuando se responde, la lectura ya está en disco. Cuerpo: `{status: "created", reading_id, station_id, received_at}`.
3. **Duplicado (`station_id` + `timestamp`) → `409`.** El índice de duplicados es un `set` en memoria que se reconstruye leyendo el archivo al arrancar. La comparación es por instante, no por texto: `21:15:00Z` y `21:15:00.000Z` son la misma lectura. **Los clientes deben tratar 409 como éxito al reintentar**, porque significa que la lectura ya estaba guardada (p. ej. si se perdió la respuesta del primer intento).
4. **`wind_direction_deg = null` con velocidad < 0.5 m/s es comportamiento del generador, no una regla de rechazo.** El API acepta `null` y también un valor numérico con velocidad baja, porque un anemómetro real puede reportar dirección con viento débil.

## Hallazgo de implementación: el cuerpo se lee a mano

El orden de comprobaciones acordado es 401 → 415 → 413 → 400 → 422 → 403 → 409 → 201. Si el cuerpo se declara como parámetro del endpoint, FastAPI (`fastapi/routing.py`, versión 0.142.2) **lee el cuerpo completo sin tope y lo parsea como JSON antes de resolver las dependencias**, autenticación incluida. Con eso, un JSON mal formado sin clave daría 400 en lugar de 401, y el límite de 4 KB solo se aplicaría después de leer todo el cuerpo.

Por eso `POST /v1/readings` hace lo siguiente:

- lee el cuerpo con `request.stream()`, con un tope de 4,096 bytes; también funciona en envíos *chunked* sin `Content-Length`;
- parsea el JSON con `json.loads` para reportar línea y columna;
- valida con `WeatherReading.model_validate_json`.

El handler de `RequestValidationError` también se sobrescribe (JSON inválido → 400, esquema → 422, mismo formato problem+json), pero `/v1/readings` no depende de él. En OpenAPI, el `requestBody` se declara manualmente con una referencia al modelo.

## Límites conocidos

- **Memoria del índice de duplicados: acotada a la ventana.** El índice solo conserva claves con timestamp dentro de la retención, que es `MAX_PAST_AGE_S` + 1 h de margen. Una clave más vieja es inalcanzable: su timestamp se rechaza con 422 antes de llegar a `save()`. La poda corre dentro de `save()`, bajo el lock, como máximo una vez cada 5 minutos, y el arranque no carga claves fuera de la retención. Con ~194 bytes por clave (medido con `tracemalloc`, Python 3.13), 5 estaciones a 1 lectura/min con la ventana de 7 días mantienen ~50,700 claves, ≈ 10 MB. Antes de la poda, el índice crecía sin límite (≈ 510 MB al año con esa misma carga).
- **El archivo sigue creciendo y el arranque lo lee completo:** la poda solo afecta la memoria. El JSONL no se rota ni se compacta, y la reconstrucción del índice recorre todas sus líneas, aunque descarta las viejas.
- **Un solo proceso:** el lock y el índice son locales. Con varios workers de uvicorn o varias réplicas, dos procesos podrían aceptar la misma lectura y escribir en el mismo archivo. En Container Apps, este adaptador exige `maxReplicas = 1`. El adaptador de Blob deberá resolver la deduplicación de otra forma (p. ej. un nombre de blob determinista por `station_id` + `timestamp` con escritura condicional).
- **Líneas corruptas:** se ignoran al reconstruir el índice y se registra un aviso con el número de línea. Si la última línea quedó truncada, se cierra con un salto antes de seguir escribiendo.
- **Escritura fallida a medias:** si `write`, `flush` o `fsync` fallan (p. ej. disco lleno), la respuesta es 500, la lectura no entra al índice y el archivo queda marcado como "sucio". La siguiente escritura exitosa antepone un salto de línea, así la lectura buena queda en su propia línea y no se pega a la parcial. La línea parcial se salta como corrupta al reconstruir. Si el que falló fue `fsync`, la línea pudo quedar completa en el archivo; como la clave no entró al índice, un reintento la vuelve a escribir. **La misma lectura puede quedar dos veces en el archivo**, aunque el índice la cuenta una sola vez. Quien consuma el JSONL debe deduplicar por `station_id` + `timestamp`. Si lo que falla a medias es el `flush` del `BufferedWriter`, Python conserva la línea completa en su búfer y la vuelve a escribir en el siguiente `flush`. Queda entonces una línea corrupta (la mitad escrita seguida de la línea entera), que se salta al reconstruir; la lectura siguiente queda íntegra en su propia línea, así que no se pierde ninguna lectura aceptada. La lectura que recibió 500 sí queda completa en el archivo, aunque dentro de esa línea corrupta: un consumidor que recupere datos de líneas dañadas debe deduplicarla igual que un reintento. Verificado con Python 3.13.5 y un `RawIOBase` que falla a medias, no con un disco lleno real.
- **Traceback del 500 en el log del servidor:** la respuesta al cliente es genérica (`"detail": "Error interno"`) y el log estructurado de la app solo registra el tipo de excepción. Pero Starlette vuelve a lanzar la excepción después de enviar el 500, así que uvicorn escribe el traceback completo en su log, **incluido el mensaje de la excepción** (verificado con `OSError("disco lleno: /secret/path")`). No se suprime porque sirve para depurar. En esa prueba, ni la clave ni los valores del payload aparecieron en el traceback; si una excepción futura incluyera datos en su mensaje, sí aparecerían.
- **Validación estricta de tipos:** `"24.7"` (string), `true` en un campo numérico y `60.0` en un campo entero (`interval_s`, `sequence`, `wind_direction_deg`) se rechazan con 422. El generador debe enviar enteros como enteros.
- **Ventana del timestamp:** depende del reloj del servidor. Se configura con `MAX_FUTURE_SKEW_S` y `MAX_PAST_AGE_S`.
