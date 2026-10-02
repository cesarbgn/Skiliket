# Revisión Fase A — lote 2 (solo archivos cambiados)

Volcado literal generado por script desde los bytes de cada archivo. Incluye solo los archivos cuyo sha256 difiere del registrado en revision-fase-a-codigo.md / revision-fase-a-pruebas-docs.md, en el orden de esos volcados. Cada bloque está delimitado por una cerca de backticks más larga que cualquier secuencia de backticks del archivo. N = líneas según `wc -l`; sha256 = hash de los bytes del archivo.

=== ingest_api/app/main.py === (259 líneas, sha256: c20acc86a1753e41ebb6f94d8f0bae836e19a86c6b09c1a549ba6dc0b0c4f69d)

```python
"""API de ingesta. Arranque: uvicorn app.main:create_app --factory"""

import json
import logging
import re
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Any

from fastapi import FastAPI, Request
from fastapi.concurrency import run_in_threadpool
from fastapi.exceptions import RequestValidationError
from fastapi.openapi.utils import get_openapi
from fastapi.responses import JSONResponse
from pydantic import ValidationError
from starlette.exceptions import HTTPException as StarletteHTTPException

from app import errors
from app.auth import STATION_KEY_HEADER, KeyVerifier
from app.config import Settings, load_settings
from app.log import configure_logging
from app.models import ReadingCreated, WeatherReading
from app.storage import DuplicateReadingError, JsonlStorage, StorageAdapter, StoredReading

logger = logging.getLogger("skilliket.ingest")

READINGS_PATH = "/v1/readings"
_JIT_POSITION = re.compile(r"line (\d+) column (\d+)")
_NON_STANDARD_CONSTANT = re.compile(r"-?Infinity|NaN")


class _NonStandardConstant(ValueError):
    pass


def _reject_constant(name: str) -> None:
    raise _NonStandardConstant(name)


def _log(status: int, reason: str, station_id: str | None = None, **extra: Any) -> None:
    level = logging.INFO if status < 400 else logging.WARNING
    logger.log(
        level,
        "reading_created" if status == 201 else "reading_rejected",
        extra={"event": "reading", "status": status, "reason": reason, "station_id": station_id, **extra},
    )


async def _read_body_capped(request: Request, limit: int) -> bytes | None:
    """Lee el cuerpo con tope; None si supera `limit` (con o sin Content-Length)."""
    declared = request.headers.get("content-length")
    if declared is not None and declared.isascii() and declared.isdigit() and int(declared) > limit:
        return None
    body = bytearray()
    async for chunk in request.stream():
        body.extend(chunk)
        if len(body) > limit:
            return None
    return bytes(body)


def _parse_json(raw: bytes) -> str | tuple[str, int | None, int | None]:
    """Devuelve el texto si es JSON sintácticamente válido, o (detalle, línea, columna)."""
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        line, column = errors.line_col(raw[: exc.start].decode("utf-8", "replace"), exc.start)
        return "El cuerpo no es UTF-8 válido", line, column
    try:
        json.loads(text, parse_constant=_reject_constant)
    except json.JSONDecodeError as exc:
        return exc.msg, exc.lineno, exc.colno
    except _NonStandardConstant as exc:
        match = _NON_STANDARD_CONSTANT.search(text)
        line, column = errors.line_col(text, match.start()) if match else (None, None)
        return f"{exc} no es un valor JSON válido", line, column
    except RecursionError:
        return "Anidamiento JSON demasiado profundo", None, None
    return text


def _is_utf8_json(content_type: str | None) -> bool:
    if not content_type:
        return False
    media_type, *params = (part.strip().lower() for part in content_type.split(";"))
    if media_type != "application/json":
        return False
    for param in params:
        name, _, value = param.partition("=")
        if name.strip() == "charset" and value.strip().strip('"') not in ("utf-8", "utf8"):
            return False
    return True


def _custom_openapi(app: FastAPI) -> dict[str, Any]:
    if app.openapi_schema:
        return app.openapi_schema
    schema = get_openapi(
        title=app.title, version=app.version, description=app.description, routes=app.routes
    )
    components = schema.setdefault("components", {})
    schemas = components.setdefault("schemas", {})
    ref = "#/components/schemas/{model}"
    for model in (WeatherReading, errors.Problem):
        model_schema = model.model_json_schema(ref_template=ref)
        schemas.update(model_schema.pop("$defs", {}))
        schemas[model.__name__] = model_schema
    components["securitySchemes"] = {
        "StationKey": {"type": "apiKey", "in": "header", "name": STATION_KEY_HEADER}
    }
    operation = schema["paths"][READINGS_PATH]["post"]
    operation["security"] = [{"StationKey": []}]
    operation["requestBody"] = {
        "required": True,
        "content": {"application/json": {"schema": {"$ref": "#/components/schemas/WeatherReading"}}},
    }
    app.openapi_schema = schema
    return schema


def _problem_response(description: str) -> dict[str, Any]:
    return {
        "description": description,
        "content": {errors.PROBLEM_MEDIA_TYPE: {"schema": {"$ref": "#/components/schemas/Problem"}}},
    }


def create_app(settings: Settings | None = None, storage: StorageAdapter | None = None) -> FastAPI:
    settings = settings or load_settings()
    configure_logging()
    verifier = KeyVerifier(settings.station_key_hashes)
    store = storage or JsonlStorage(settings.storage_path, settings.max_past_age)

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        yield
        store.close()

    app = FastAPI(
        title="Skilliket — API de ingesta meteorológica",
        version="1.0.0",
        description="Recibe y valida lecturas de estaciones meteorológicas (esquema v1).",
        lifespan=lifespan,
    )
    app.add_exception_handler(StarletteHTTPException, errors.http_exception_handler)
    app.add_exception_handler(RequestValidationError, errors.request_validation_handler)
    app.add_exception_handler(Exception, errors.unhandled_exception_handler)
    app.openapi = lambda: _custom_openapi(app)  # type: ignore[method-assign]

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    # El cuerpo se lee a mano: si se declarara como parámetro, FastAPI lo leería completo
    # y lo parsearía antes de la autenticación, rompiendo el orden 401 → 415 → 413 → 400.
    @app.post(
        READINGS_PATH,
        status_code=201,
        response_model=ReadingCreated,
        responses={
            400: _problem_response("JSON mal formado (incluye line/column)"),
            401: _problem_response("Falta X-Station-Key o no es válida"),
            403: _problem_response("station_id del cuerpo no corresponde a la clave"),
            409: _problem_response("Lectura duplicada (station_id + timestamp); tratar como éxito"),
            413: _problem_response("Cuerpo mayor a 4 KB"),
            415: _problem_response("Content-Type distinto de application/json"),
            422: _problem_response("El payload no cumple el esquema v1 (incluye errors[])"),
        },
    )
    async def create_reading(request: Request) -> JSONResponse:
        path = request.url.path

        station_id = verifier.station_for_key(request.headers.get(STATION_KEY_HEADER))
        if station_id is None:
            _log(401, "invalid_or_missing_key")
            return errors.problem(
                401, "unauthorized", "Unauthorized", f"Falta {STATION_KEY_HEADER} o no es válida", path
            )

        if not _is_utf8_json(request.headers.get("content-type")):
            _log(415, "unsupported_media_type", station_id)
            return errors.problem(
                415,
                "unsupported-media-type",
                "Unsupported Media Type",
                "Content-Type debe ser application/json (UTF-8)",
                path,
            )

        raw = await _read_body_capped(request, settings.max_body_bytes)
        if raw is None:
            _log(413, "payload_too_large", station_id)
            return errors.problem(
                413,
                "payload-too-large",
                "Payload Too Large",
                f"El cuerpo supera {settings.max_body_bytes} bytes",
                path,
            )

        parsed = _parse_json(raw)
        if not isinstance(parsed, str):
            detail, line, column = parsed
            _log(400, "malformed_json", station_id)
            return errors.malformed_json(path, detail, line, column)

        context = {
            "now": datetime.now(UTC),
            "max_future_skew": settings.max_future_skew,
            "max_past_age": settings.max_past_age,
        }
        try:
            reading = WeatherReading.model_validate_json(parsed, context=context)
        except ValidationError as exc:
            exc_errors = exc.errors()
            invalid = next((e for e in exc_errors if e["type"] == "json_invalid"), None)
            if invalid is not None:
                # El parser de pydantic es más estricto que json.loads en casos raros.
                position = _JIT_POSITION.search(invalid["msg"])
                line, column = (int(position[1]), int(position[2])) if position else (None, None)
                _log(400, "malformed_json", station_id)
                return errors.malformed_json(path, invalid["msg"], line, column)
            field_errors = errors.field_errors(exc_errors)
            _log(422, "validation_error", station_id, codes=sorted({e["code"] for e in field_errors}))
            return errors.validation_failed(path, field_errors)

        if reading.station_id != station_id:
            _log(403, "station_mismatch", station_id)
            return errors.problem(
                403, "forbidden", "Forbidden", "La clave no corresponde al station_id del cuerpo", path
            )

        stored = StoredReading(
            reading_id=str(uuid.uuid4()), received_at=datetime.now(UTC), reading=reading
        )
        try:
            await run_in_threadpool(store.save, stored)
        except DuplicateReadingError:
            _log(409, "duplicate", station_id)
            return errors.problem(
                409,
                "duplicate-reading",
                "Conflict",
                "Ya existe una lectura con este station_id y timestamp; al reintentar, trátalo como éxito",
                path,
            )

        _log(201, "created", station_id, reading_id=stored.reading_id)
        body = ReadingCreated(
            status="created",
            reading_id=stored.reading_id,
            station_id=station_id,
            received_at=stored.to_record()["received_at"],
        )
        return JSONResponse(body.model_dump(), status_code=201)

    return app
```

=== ingest_api/app/errors.py === (151 líneas, sha256: 731f7ea67c2f22a834aa7399ca7e139e03367e7e008347ef06fa7150cc8d60cd)

```python
"""Respuestas de error RFC 9457 (application/problem+json)."""

import logging
from collections.abc import Iterable, Mapping, Sequence
from datetime import datetime
from http import HTTPStatus
from typing import Any

from fastapi import Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from starlette.exceptions import HTTPException as StarletteHTTPException

logger = logging.getLogger("skilliket.ingest")

PROBLEM_MEDIA_TYPE = "application/problem+json"
_MAX_ECHO_LEN = 100


class FieldError(BaseModel):
    field: str
    code: str
    message: str
    value: Any = None


class Problem(BaseModel):
    type: str
    title: str
    status: int
    detail: str
    instance: str | None = None
    line: int | None = None
    column: int | None = None
    errors: list[FieldError] | None = None


def problem(
    status: int,
    slug: str,
    title: str,
    detail: str,
    instance: str | None = None,
    headers: Mapping[str, str] | None = None,
    **extra: Any,
) -> JSONResponse:
    body: dict[str, Any] = {
        "type": f"urn:skilliket:problem:{slug}",
        "title": title,
        "status": status,
        "detail": detail,
    }
    if instance:
        body["instance"] = instance
    body.update(extra)
    return JSONResponse(
        body, status_code=status, media_type=PROBLEM_MEDIA_TYPE, headers=dict(headers or {})
    )


def malformed_json(instance: str, detail: str, line: int | None, column: int | None) -> JSONResponse:
    return problem(400, "malformed-json", "Malformed JSON", detail, instance, line=line, column=column)


def validation_failed(instance: str, errors: list[dict[str, Any]]) -> JSONResponse:
    return problem(
        422,
        "validation-error",
        "Validation failed",
        f"El payload no cumple el esquema v1 ({len(errors)} error(es))",
        instance,
        errors=errors,
    )


def line_col(text: str, pos: int) -> tuple[int, int]:
    """Línea y columna (base 1) de un desplazamiento en el texto."""
    line = text.count("\n", 0, pos) + 1
    column = pos - (text.rfind("\n", 0, pos) + 1) + 1
    return line, column


def _safe_value(error: dict[str, Any]) -> Any:
    # Nunca se devuelven objetos completos (evita reflejar el payload entero).
    if error["type"] == "missing":
        return None
    value = error.get("input")
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, str):
        return value[:_MAX_ECHO_LEN]
    if value is None or isinstance(value, (bool, int, float)):
        return value
    return None


def field_errors(errors: Iterable[dict[str, Any]], strip_body: bool = False) -> list[dict[str, Any]]:
    result = []
    for error in errors:
        loc: Sequence[Any] = error["loc"]
        if strip_body and loc[:1] == ("body",):
            loc = loc[1:]
        result.append(
            {
                "field": ".".join(str(part) for part in loc) or "(body)",
                "code": error["type"],
                "message": error["msg"],
                "value": _safe_value(error),
            }
        )
    return result


async def request_validation_handler(request: Request, exc: RequestValidationError) -> JSONResponse:
    """Sustituye el 422 genérico de FastAPI: JSON mal formado → 400, esquema → 422."""
    errors = exc.errors()
    for error in errors:
        if error["type"] == "json_invalid":
            line = column = None
            loc = error.get("loc", ())
            body = exc.body
            if isinstance(body, bytes):
                body = body.decode("utf-8", errors="replace")
            if len(loc) > 1 and isinstance(loc[1], int) and isinstance(body, str):
                line, column = line_col(body, loc[1])
            detail = error.get("ctx", {}).get("error") or "JSON mal formado"
            return malformed_json(request.url.path, detail, line, column)
    return validation_failed(request.url.path, field_errors(errors, strip_body=True))


async def http_exception_handler(request: Request, exc: StarletteHTTPException) -> JSONResponse:
    """404, 405 y demás errores HTTP del framework en problem+json, con sus cabeceras (p. ej. Allow)."""
    try:
        phrase = HTTPStatus(exc.status_code).phrase
    except ValueError:  # código fuera del estándar
        phrase = "HTTP Error"
    detail = exc.detail if isinstance(exc.detail, str) else phrase
    slug = phrase.lower().replace(" ", "-")
    return problem(exc.status_code, slug, phrase, detail, request.url.path, headers=exc.headers)


async def unhandled_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    # El log estructurado lleva solo el tipo de excepción. Ojo: Starlette vuelve a lanzar la
    # excepción después de enviar esta respuesta, así que uvicorn igual escribe el traceback
    # completo (con el mensaje de la excepción) en su propio log.
    logger.error(
        "unhandled_error",
        extra={"event": "unhandled_error", "status": 500, "reason": type(exc).__name__},
    )
    return problem(500, "internal-error", "Internal Server Error", "Error interno", request.url.path)
```

=== ingest_api/app/storage.py === (152 líneas, sha256: aaa08548264e833198fe9e073e7407b992c6cb3f66c08e948368ca0be9737a1f)

```python
"""Persistencia de lecturas validadas."""

import json
import logging
import os
import threading
from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from app.models import DEFAULT_MAX_PAST_AGE, WeatherReading

logger = logging.getLogger("skilliket.ingest.storage")

DedupKey = tuple[str, datetime]

# Margen sobre max_past_age antes de olvidar una clave, y frecuencia máxima de la poda.
RETENTION_MARGIN = timedelta(hours=1)
PRUNE_INTERVAL = timedelta(minutes=5)


class DuplicateReadingError(Exception):
    """Ya existe una lectura con el mismo (station_id, timestamp)."""


@dataclass(frozen=True)
class StoredReading:
    reading_id: str
    received_at: datetime
    reading: WeatherReading

    @property
    def dedup_key(self) -> DedupKey:
        return (self.reading.station_id, self.reading.timestamp)

    def to_record(self) -> dict[str, Any]:
        return {
            "reading_id": self.reading_id,
            "received_at": self.received_at.isoformat().replace("+00:00", "Z"),
            "reading": self.reading.model_dump(mode="json", exclude_unset=True),
        }


class StorageAdapter(ABC):
    @abstractmethod
    def save(self, stored: StoredReading) -> None:
        """Persiste la lectura de forma síncrona.

        Lanza DuplicateReadingError si (station_id, timestamp) ya existe."""

    def close(self) -> None:
        """Libera recursos; opcional para implementaciones que no tienen."""


class JsonlStorage(StorageAdapter):
    """Una línea JSON por lectura en un archivo local.

    El índice de duplicados vive en memoria y se reconstruye leyendo el archivo al
    arrancar. Solo guarda claves dentro de la retención (max_past_age + margen): una
    clave más vieja es inalcanzable porque su timestamp ya se rechaza con 422.
    Válido solo para un proceso: varios workers no comparten el lock ni el índice."""

    def __init__(
        self,
        path: Path,
        max_past_age: timedelta = DEFAULT_MAX_PAST_AGE,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._path = path
        self._lock = threading.Lock()
        self._clock = clock
        self._retention = max_past_age + RETENTION_MARGIN
        self._last_prune = clock()
        # True tras un fallo de escritura: puede haber quedado una línea parcial sin salto.
        self._dirty = False
        path.parent.mkdir(parents=True, exist_ok=True)
        self._index: set[DedupKey] = self._rebuild_index()
        needs_newline = self._ends_without_newline()
        self._file = open(path, "ab")  # se cierra en close()
        if needs_newline:
            # Una última línea truncada (p. ej. tras un corte) no debe pegarse con la siguiente.
            self._file.write(b"\n")
            self._file.flush()

    @property
    def indexed_count(self) -> int:
        return len(self._index)

    def _rebuild_index(self) -> set[DedupKey]:
        index: set[DedupKey] = set()
        if not self._path.exists():
            return index
        cutoff = self._clock() - self._retention
        with open(self._path, "rb") as f:
            for lineno, line in enumerate(f, start=1):
                if not line.strip():
                    continue
                try:
                    reading = json.loads(line)["reading"]
                    key = (reading["station_id"], datetime.fromisoformat(reading["timestamp"]))
                    if key[1] >= cutoff:
                        index.add(key)
                except (ValueError, KeyError, TypeError):
                    logger.warning(
                        "storage_line_skipped", extra={"event": "storage_line_skipped", "line": lineno}
                    )
        return index

    def _prune_locked(self, now: datetime) -> None:
        """Quita las claves fuera de la retención. Requiere tener el lock."""
        cutoff = now - self._retention
        self._index = {key for key in self._index if key[1] >= cutoff}
        self._last_prune = now

    def _ends_without_newline(self) -> bool:
        if not self._path.exists() or self._path.stat().st_size == 0:
            return False
        with open(self._path, "rb") as f:
            f.seek(-1, os.SEEK_END)
            return f.read(1) != b"\n"

    def save(self, stored: StoredReading) -> None:
        line = json.dumps(stored.to_record(), ensure_ascii=False, separators=(",", ":"))
        data = line.encode("utf-8") + b"\n"
        with self._lock:
            now = self._clock()
            # La poda recorre todo el índice: como máximo una vez por intervalo, no en cada escritura.
            if now - self._last_prune >= PRUNE_INTERVAL:
                self._prune_locked(now)
            if stored.dedup_key in self._index:
                raise DuplicateReadingError
            if self._dirty:
                # Cierra la posible línea parcial del fallo anterior para no pegarse a ella.
                data = b"\n" + data
            # Una sola escritura por línea completa bajo el lock: sin intercalado entre hilos.
            try:
                self._file.write(data)
                self._file.flush()
                os.fsync(self._file.fileno())
            except BaseException:
                self._dirty = True
                raise
            self._dirty = False
            self._index.add(stored.dedup_key)

    def close(self) -> None:
        with self._lock:
            if not self._file.closed:
                self._file.close()
```

=== ingest_api/app/models.py === (134 líneas, sha256: b2e7078d41eee63b12e47596d0e84b277884d680559b34e9985f17bd1a8ed8ff)

```python
"""Contrato v1 de una lectura meteorológica (fuente única de verdad del esquema)."""

import re
from datetime import UTC, datetime, timedelta
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, field_validator
from pydantic_core import PydanticCustomError

SCHEMA_VERSION = "1.0"
# [0-9] y no \d: en Python (y en el motor de pydantic) \d acepta dígitos Unicode como "٠",
# mientras que en el JSON Schema publicado (ECMAScript) solo acepta ASCII.
STATION_ID_PATTERN = r"^[A-Z]{3}-[0-9]{3}$"
TIMESTAMP_PATTERN = r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]{1,6})?Z$"
DEFAULT_MAX_FUTURE_SKEW = timedelta(minutes=5)
DEFAULT_MAX_PAST_AGE = timedelta(days=7)

_TIMESTAMP_RE = re.compile(TIMESTAMP_PATTERN)


class _StrictModel(BaseModel):
    # strict: "24.5" no se convierte a número; extra="forbid": campos desconocidos se rechazan.
    model_config = ConfigDict(extra="forbid", strict=True, allow_inf_nan=False)


class Measurements(_StrictModel):
    air_temperature_c: float = Field(ge=-40, le=60, description="Temperatura del aire, °C")
    relative_humidity_pct: float = Field(ge=0, le=100, description="Humedad relativa, %")
    station_pressure_hpa: float = Field(
        ge=500, le=1100, description="Presión a la altura de la estación, hPa"
    )
    wind_speed_ms: float = Field(ge=0, le=75, description="Velocidad media del viento, m/s")
    wind_gust_ms: float | None = Field(
        default=None, ge=0, le=100, description="Ráfaga máxima del intervalo, m/s; >= wind_speed_ms"
    )
    wind_direction_deg: int | None = Field(
        default=None,
        ge=0,
        le=359,
        description="Dirección de procedencia del viento, grados; null admitido (calma)",
    )
    precipitation_mm: float = Field(
        ge=0, le=200, description="Precipitación acumulada en el intervalo, mm"
    )
    solar_radiation_wm2: float | None = Field(
        default=None, ge=0, le=1500, description="Radiación solar global, W/m²"
    )

    @field_validator("wind_gust_ms")
    @classmethod
    def _gust_not_below_speed(cls, gust: float | None, info: ValidationInfo) -> float | None:
        # wind_speed_ms se declara antes, así que ya está en info.data si fue válido.
        speed = info.data.get("wind_speed_ms")
        if gust is not None and speed is not None and gust < speed:
            raise PydanticCustomError(
                "gust_less_than_speed",
                "wind_gust_ms ({gust}) debe ser mayor o igual que wind_speed_ms ({speed})",
                {"gust": gust, "speed": speed},
            )
        return gust


class Device(_StrictModel):
    battery_v: float | None = Field(default=None, ge=0, le=15, description="Voltaje de batería, V")
    firmware: str | None = Field(default=None, min_length=1, max_length=64)


class WeatherReading(_StrictModel):
    """Lectura de una estación meteorológica, esquema v1. Reglas que JSON Schema no
    expresa y el API sí aplica: wind_gust_ms >= wind_speed_ms y timestamp dentro de
    la ventana configurada (por defecto <= 5 min en el futuro y <= 7 días en el pasado)."""

    # La ventana del timestamp se puede ajustar con el contexto de validación:
    # `now`, `max_future_skew` y `max_past_age`.
    model_config = ConfigDict(title="WeatherReading")

    schema_version: Literal["1.0"]
    station_id: str = Field(pattern=STATION_ID_PATTERN, examples=["GDL-001"])
    timestamp: datetime = Field(
        description="Instante de la medición, RFC 3339 en UTC con sufijo Z",
        json_schema_extra={"pattern": TIMESTAMP_PATTERN},
        examples=["2026-10-01T21:15:00Z"],
    )
    sequence: int | None = Field(default=None, ge=0, description="Contador para detectar huecos")
    interval_s: int = Field(ge=10, le=3600, description="Duración del intervalo medido, s")
    measurements: Measurements
    device: Device | None = None

    @field_validator("timestamp", mode="before")
    @classmethod
    def _require_utc_z(cls, value: Any) -> datetime:
        if isinstance(value, str) and _TIMESTAMP_RE.fullmatch(value):
            try:
                return datetime.fromisoformat(value)
            except ValueError:
                pass  # p. ej. mes 13: cae al error de formato
        raise PydanticCustomError(
            "timestamp_format",
            "timestamp debe ser RFC 3339 en UTC con sufijo Z, p. ej. 2026-10-01T21:15:00Z",
        )

    @field_validator("timestamp")
    @classmethod
    def _within_window(cls, ts: datetime, info: ValidationInfo) -> datetime:
        ctx = info.context or {}
        now: datetime = ctx.get("now") or datetime.now(UTC)
        max_future: timedelta = ctx.get("max_future_skew", DEFAULT_MAX_FUTURE_SKEW)
        max_past: timedelta = ctx.get("max_past_age", DEFAULT_MAX_PAST_AGE)
        if ts > now + max_future:
            raise PydanticCustomError(
                "timestamp_in_future",
                "timestamp está más de {seconds} s en el futuro",
                {"seconds": int(max_future.total_seconds())},
            )
        if ts < now - max_past:
            raise PydanticCustomError(
                "timestamp_too_old",
                "timestamp tiene más de {seconds} s de antigüedad",
                {"seconds": int(max_past.total_seconds())},
            )
        return ts


class ReadingCreated(BaseModel):
    status: Literal["created"]
    reading_id: str
    station_id: str
    received_at: str


def weather_reading_json_schema() -> dict[str, Any]:
    """Esquema JSON publicado en schema/weather_reading.v1.schema.json."""
    schema = WeatherReading.model_json_schema()
    return {"$schema": "https://json-schema.org/draft/2020-12/schema", **schema}
```

=== schema/weather_reading.v1.schema.json === (206 líneas, sha256: f01ae8f9fce5b4f184bcbffa5f254b72d2823520added38678d7998f34099001)

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$defs": {
    "Device": {
      "additionalProperties": false,
      "properties": {
        "battery_v": {
          "anyOf": [
            {
              "maximum": 15,
              "minimum": 0,
              "type": "number"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Voltaje de batería, V",
          "title": "Battery V"
        },
        "firmware": {
          "anyOf": [
            {
              "maxLength": 64,
              "minLength": 1,
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "title": "Firmware"
        }
      },
      "title": "Device",
      "type": "object"
    },
    "Measurements": {
      "additionalProperties": false,
      "properties": {
        "air_temperature_c": {
          "description": "Temperatura del aire, °C",
          "maximum": 60,
          "minimum": -40,
          "title": "Air Temperature C",
          "type": "number"
        },
        "relative_humidity_pct": {
          "description": "Humedad relativa, %",
          "maximum": 100,
          "minimum": 0,
          "title": "Relative Humidity Pct",
          "type": "number"
        },
        "station_pressure_hpa": {
          "description": "Presión a la altura de la estación, hPa",
          "maximum": 1100,
          "minimum": 500,
          "title": "Station Pressure Hpa",
          "type": "number"
        },
        "wind_speed_ms": {
          "description": "Velocidad media del viento, m/s",
          "maximum": 75,
          "minimum": 0,
          "title": "Wind Speed Ms",
          "type": "number"
        },
        "wind_gust_ms": {
          "anyOf": [
            {
              "maximum": 100,
              "minimum": 0,
              "type": "number"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Ráfaga máxima del intervalo, m/s; >= wind_speed_ms",
          "title": "Wind Gust Ms"
        },
        "wind_direction_deg": {
          "anyOf": [
            {
              "maximum": 359,
              "minimum": 0,
              "type": "integer"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Dirección de procedencia del viento, grados; null admitido (calma)",
          "title": "Wind Direction Deg"
        },
        "precipitation_mm": {
          "description": "Precipitación acumulada en el intervalo, mm",
          "maximum": 200,
          "minimum": 0,
          "title": "Precipitation Mm",
          "type": "number"
        },
        "solar_radiation_wm2": {
          "anyOf": [
            {
              "maximum": 1500,
              "minimum": 0,
              "type": "number"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Radiación solar global, W/m²",
          "title": "Solar Radiation Wm2"
        }
      },
      "required": [
        "air_temperature_c",
        "relative_humidity_pct",
        "station_pressure_hpa",
        "wind_speed_ms",
        "precipitation_mm"
      ],
      "title": "Measurements",
      "type": "object"
    }
  },
  "additionalProperties": false,
  "description": "Lectura de una estación meteorológica, esquema v1. Reglas que JSON Schema no\nexpresa y el API sí aplica: wind_gust_ms >= wind_speed_ms y timestamp dentro de\nla ventana configurada (por defecto <= 5 min en el futuro y <= 7 días en el pasado).",
  "properties": {
    "schema_version": {
      "const": "1.0",
      "title": "Schema Version",
      "type": "string"
    },
    "station_id": {
      "examples": [
        "GDL-001"
      ],
      "pattern": "^[A-Z]{3}-[0-9]{3}$",
      "title": "Station Id",
      "type": "string"
    },
    "timestamp": {
      "description": "Instante de la medición, RFC 3339 en UTC con sufijo Z",
      "examples": [
        "2026-10-01T21:15:00Z"
      ],
      "format": "date-time",
      "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\\.[0-9]{1,6})?Z$",
      "title": "Timestamp",
      "type": "string"
    },
    "sequence": {
      "anyOf": [
        {
          "minimum": 0,
          "type": "integer"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Contador para detectar huecos",
      "title": "Sequence"
    },
    "interval_s": {
      "description": "Duración del intervalo medido, s",
      "maximum": 3600,
      "minimum": 10,
      "title": "Interval S",
      "type": "integer"
    },
    "measurements": {
      "$ref": "#/$defs/Measurements"
    },
    "device": {
      "anyOf": [
        {
          "$ref": "#/$defs/Device"
        },
        {
          "type": "null"
        }
      ],
      "default": null
    }
  },
  "required": [
    "schema_version",
    "station_id",
    "timestamp",
    "interval_s",
    "measurements"
  ],
  "title": "WeatherReading",
  "type": "object"
}
```

=== ingest_api/tests/conftest.py === (118 líneas, sha256: 59757d6ffc1e50a16a507f902da43b3481d900c5ba63a204ae91ddfe250a8f60)

```python
import copy
import logging
import secrets
from collections.abc import Callable, Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient

from app.auth import hash_key
from app.config import Settings
from app.main import create_app

# Claves generadas en cada ejecución: ninguna clave queda escrita en archivos versionados.
KEYS = {
    "GDL-001": (secrets.token_urlsafe(32), secrets.token_urlsafe(32)),
    "GDL-002": (secrets.token_urlsafe(32),),
}


class FakeClock:
    """Reloj inyectable para JsonlStorage."""

    def __init__(self, now: datetime) -> None:
        self.now = now

    def __call__(self) -> datetime:
        return self.now

    def advance(self, **delta: float) -> None:
        self.now += timedelta(**delta)


def iso_z(dt: datetime) -> str:
    return dt.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def make_payload(station_id: str = "GDL-001", timestamp: datetime | None = None) -> dict[str, Any]:
    return {
        "schema_version": "1.0",
        "station_id": station_id,
        "timestamp": iso_z(timestamp or datetime.now(UTC) - timedelta(seconds=30)),
        "sequence": 1834,
        "interval_s": 60,
        "measurements": {
            "air_temperature_c": 24.7,
            "relative_humidity_pct": 58,
            "station_pressure_hpa": 841.3,
            "wind_speed_ms": 3.4,
            "wind_gust_ms": 5.1,
            "wind_direction_deg": 245,
            "precipitation_mm": 0.0,
            "solar_radiation_wm2": 612,
        },
        "device": {"battery_v": 13.21, "firmware": "sim-0.1.0"},
    }


@pytest.fixture
def payload() -> dict[str, Any]:
    return make_payload()


@pytest.fixture
def mutate(payload: dict[str, Any]) -> Callable[[Callable[[dict[str, Any]], None]], dict[str, Any]]:
    def _mutate(change: Callable[[dict[str, Any]], None]) -> dict[str, Any]:
        data = copy.deepcopy(payload)
        change(data)
        return data

    return _mutate


@pytest.fixture
def storage_path(tmp_path: Path) -> Path:
    return tmp_path / "data" / "readings.jsonl"


@pytest.fixture
def settings(storage_path: Path) -> Settings:
    return Settings(
        station_key_hashes={sid: tuple(hash_key(k) for k in keys) for sid, keys in KEYS.items()},
        storage_path=storage_path,
        max_future_skew=timedelta(minutes=5),
        max_past_age=timedelta(days=7),
    )


@pytest.fixture
def client(settings: Settings) -> Iterator[TestClient]:
    with TestClient(create_app(settings)) as test_client:
        yield test_client


@pytest.fixture
def auth() -> dict[str, str]:
    return {"X-Station-Key": KEYS["GDL-001"][0]}


class _ListHandler(logging.Handler):
    def __init__(self) -> None:
        super().__init__()
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)


@pytest.fixture
def log_records() -> Iterator[list[logging.LogRecord]]:
    # El logger "skilliket" no propaga a root, así que caplog no lo ve.
    handler = _ListHandler()
    logger = logging.getLogger("skilliket")
    logger.addHandler(handler)
    yield handler.records
    logger.removeHandler(handler)
```

=== ingest_api/tests/test_auth.py === (73 líneas, sha256: 3bac2b98f01294aad2141024b332c4b15fad5e621a676d671dbd9537b94a4af0)

```python
import json

import pytest

from app.auth import KeyVerifier, hash_key
from app.config import ConfigError, load_settings, parse_station_key_hashes

H1, H2, H3 = (hash_key(k) for k in ("a", "b", "c"))


def test_both_station_keys_resolve_to_station() -> None:
    verifier = KeyVerifier({"GDL-001": (hash_key("k1"), hash_key("k2")), "GDL-002": (hash_key("k3"),)})
    assert verifier.station_for_key("k1") == "GDL-001"
    assert verifier.station_for_key("k2") == "GDL-001"
    assert verifier.station_for_key("k3") == "GDL-002"


@pytest.mark.parametrize("key", [None, "", "k4"])
def test_unknown_or_missing_key_is_rejected(key) -> None:
    assert KeyVerifier({"GDL-001": (hash_key("k1"),)}).station_for_key(key) is None


def test_wrong_hash_rejects_its_key() -> None:
    wrong = hash_key("k1")[:-1] + ("0" if hash_key("k1")[-1] != "0" else "1")
    assert KeyVerifier({"GDL-001": (wrong,)}).station_for_key("k1") is None


def test_parse_valid_hashes() -> None:
    assert parse_station_key_hashes(json.dumps({"GDL-001": [H1, H2]})) == {"GDL-001": (H1, H2)}


@pytest.mark.parametrize(
    "raw",
    [
        "not json",
        "{}",
        "[]",
        json.dumps({"gdl-001": [H1]}),
        json.dumps({"GDL-٠٠١": [H1]}),
        json.dumps({"GDL-001": []}),
        json.dumps({"GDL-001": [H1, H2, H3]}),
        json.dumps({"GDL-001": [H1.upper()]}),
        json.dumps({"GDL-001": ["abc"]}),
        json.dumps({"GDL-001": [H1, H1]}),
        json.dumps({"GDL-001": [H1], "GDL-002": [H1]}),
        json.dumps({"GDL-001": H1}),
    ],
    ids=[
        "not-json", "empty", "not-object", "bad-station-id", "unicode-digit-station-id",
        "no-hashes", "three-hashes",
        "uppercase-hex", "short-hash", "repeated-in-station", "shared-across-stations", "not-list",
    ],
)
def test_invalid_hash_config_is_rejected(raw: str) -> None:
    with pytest.raises(ConfigError):
        parse_station_key_hashes(raw)


def test_settings_require_station_hashes(monkeypatch) -> None:
    monkeypatch.delenv("STATION_KEY_HASHES", raising=False)
    with pytest.raises(ConfigError):
        load_settings()


def test_settings_from_env(monkeypatch) -> None:
    monkeypatch.setenv("STATION_KEY_HASHES", json.dumps({"GDL-001": [H1]}))
    monkeypatch.setenv("MAX_FUTURE_SKEW_S", "60")
    monkeypatch.delenv("STORAGE_PATH", raising=False)
    monkeypatch.delenv("MAX_PAST_AGE_S", raising=False)
    settings = load_settings()
    assert settings.max_future_skew.total_seconds() == 60
    assert settings.max_past_age.total_seconds() == 7 * 24 * 3600
    assert settings.storage_path.as_posix() == "data/readings.jsonl"
```

=== ingest_api/tests/test_readings.py === (358 líneas, sha256: b3be802988abaee551ac64d65dc1dd748abb9fc8221d9a15055cefcc886c5e48)

```python
import json
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta

import pytest
from fastapi.testclient import TestClient

from app.config import Settings
from app.main import create_app
from app.models import WeatherReading
from app.storage import JsonlStorage, StorageAdapter, StoredReading
from tests.conftest import KEYS, FakeClock, iso_z, make_payload

URL = "/v1/readings"
PROBLEM = "application/problem+json"


def post(client: TestClient, body: object, key: str | None = KEYS["GDL-001"][0], **kwargs):
    headers = {"Content-Type": "application/json", **kwargs.pop("headers", {})}
    if key is not None:
        headers["X-Station-Key"] = key
    content = body if isinstance(body, (bytes, str)) else json.dumps(body)
    return client.post(URL, content=content, headers=headers, **kwargs)


def lines(path) -> list[dict]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line]


# --- 201 / 409 -------------------------------------------------------------------


def test_valid_payload_is_created_and_persisted(client, payload, storage_path) -> None:
    response = post(client, payload)
    assert response.status_code == 201
    body = response.json()
    assert body["status"] == "created"
    assert body["station_id"] == "GDL-001"
    assert body["received_at"].endswith("Z")
    [record] = lines(storage_path)
    assert record["reading_id"] == body["reading_id"]
    assert record["reading"] == payload


def test_duplicate_is_409(client, payload, storage_path) -> None:
    assert post(client, payload).status_code == 201
    response = post(client, payload)
    assert response.status_code == 409
    assert response.headers["content-type"] == PROBLEM
    assert len(lines(storage_path)) == 1


def test_duplicate_detection_uses_instant_not_text(client, payload) -> None:
    assert post(client, payload).status_code == 201
    payload["timestamp"] = payload["timestamp"].replace("Z", ".000Z")
    assert post(client, payload).status_code == 409


def test_index_is_rebuilt_after_restart(settings: Settings, payload) -> None:
    with TestClient(create_app(settings)) as first:
        assert post(first, payload).status_code == 201
    with TestClient(create_app(settings)) as second:
        assert post(second, payload).status_code == 409


def test_key_inside_window_survives_pruning(settings: Settings, payload, monkeypatch) -> None:
    clock = FakeClock(datetime.now(UTC))
    storage = JsonlStorage(settings.storage_path, settings.max_past_age, clock=clock)
    prunes: list[datetime] = []
    original = storage._prune_locked
    monkeypatch.setattr(storage, "_prune_locked", lambda now: (prunes.append(now), original(now)))
    with TestClient(create_app(settings, storage=storage)) as client:
        assert post(client, payload).status_code == 201
        clock.advance(minutes=6)
        other = make_payload(timestamp=datetime.now(UTC) - timedelta(seconds=90))
        assert post(client, other).status_code == 201  # esta escritura dispara la poda
        assert len(prunes) == 1
        assert post(client, payload).status_code == 409


@pytest.mark.parametrize(
    ("speed", "direction"), [(0.0, None), (0.2, 90)], ids=["null-direction", "numeric-direction"]
)
def test_low_wind_direction_cases_are_created(client, mutate, speed, direction) -> None:
    data = mutate(
        lambda d: d["measurements"].update(
            wind_speed_ms=speed, wind_gust_ms=speed, wind_direction_deg=direction
        )
    )
    assert post(client, data).status_code == 201


# --- 401 / 403 -------------------------------------------------------------------


def test_missing_key_is_401(client, payload) -> None:
    response = post(client, payload, key=None)
    assert response.status_code == 401
    assert response.headers["content-type"] == PROBLEM
    assert response.json()["status"] == 401


def test_invalid_key_is_401(client, payload) -> None:
    assert post(client, payload, key="not-a-real-key").status_code == 401


def test_both_keys_of_a_station_are_accepted(client) -> None:
    now = datetime.now(UTC)
    first = make_payload(timestamp=now - timedelta(seconds=10))
    second = make_payload(timestamp=now - timedelta(seconds=20))
    assert post(client, first, key=KEYS["GDL-001"][0]).status_code == 201
    assert post(client, second, key=KEYS["GDL-001"][1]).status_code == 201


def test_key_of_other_station_is_403(client, payload) -> None:
    response = post(client, payload, key=KEYS["GDL-002"][0])
    assert response.status_code == 403
    assert response.headers["content-type"] == PROBLEM


# --- 415 / 413 / 400 -------------------------------------------------------------


@pytest.mark.parametrize(
    "content_type", ["text/plain", "application/xml", "application/json; charset=latin-1"]
)
def test_unsupported_content_type_is_415(client, payload, content_type) -> None:
    response = post(client, payload, headers={"Content-Type": content_type})
    assert response.status_code == 415


def test_json_with_utf8_charset_is_accepted(client, payload) -> None:
    response = post(client, payload, headers={"Content-Type": "application/json; charset=utf-8"})
    assert response.status_code == 201


def _oversized() -> bytes:
    return json.dumps({"padding": "x" * 5000}).encode()


def test_body_over_4kb_is_413(client) -> None:
    assert post(client, _oversized()).status_code == 413


def test_chunked_body_over_4kb_without_content_length_is_413(client) -> None:
    data = _oversized()

    def chunks() -> Iterator[bytes]:
        for i in range(0, len(data), 512):
            yield data[i : i + 512]

    response = client.post(
        URL,
        content=chunks(),
        headers={"Content-Type": "application/json", "X-Station-Key": KEYS["GDL-001"][0]},
    )
    assert response.status_code == 413


def test_body_of_4097_bytes_is_413(client) -> None:
    data = json.dumps(make_payload())
    data = data[:-1] + " " * (4097 - len(data)) + "}"
    assert len(data.encode()) == 4097
    assert post(client, data).status_code == 413


def test_body_of_exactly_4096_bytes_is_not_413(client) -> None:
    data = json.dumps(make_payload())
    data = data[:-1] + " " * (4096 - len(data)) + "}"
    assert len(data.encode()) == 4096
    assert post(client, data).status_code == 201


@pytest.mark.parametrize(
    ("raw", "line", "column"),
    [
        ('{"schema_version": "1.0",\n  "station_id": }', 2, 17),
        ("", 1, 1),
        ('{"a": NaN}', 1, 7),
    ],
    ids=["missing-value", "empty-body", "nan"],
)
def test_malformed_json_is_400_with_position(client, raw, line, column) -> None:
    response = post(client, raw)
    assert response.status_code == 400
    assert response.headers["content-type"] == PROBLEM
    body = response.json()
    assert (body["line"], body["column"]) == (line, column)


def test_invalid_utf8_is_400(client) -> None:
    response = post(client, b'{"a": "\xff"}')
    assert response.status_code == 400
    assert (response.json()["line"], response.json()["column"]) == (1, 8)


def test_deeply_nested_json_is_400(client) -> None:
    assert post(client, "[" * 2000 + "]" * 2000).status_code == 400


# --- 422 -------------------------------------------------------------------------


def test_schema_error_is_422_with_field_errors(client, mutate) -> None:
    data = mutate(lambda d: d["measurements"].update(relative_humidity_pct=101))
    response = post(client, data)
    assert response.status_code == 422
    assert response.headers["content-type"] == PROBLEM
    assert response.json()["errors"] == [
        {
            "field": "measurements.relative_humidity_pct",
            "code": "less_than_equal",
            "message": "Input should be less than or equal to 100",
            "value": 101,
        }
    ]


def test_missing_field_error_does_not_echo_payload(client, mutate) -> None:
    data = mutate(lambda d: d.pop("measurements"))
    [error] = post(client, data).json()["errors"]
    assert (error["field"], error["code"], error["value"]) == ("measurements", "missing", None)


def test_non_object_body_is_422(client) -> None:
    [error] = post(client, [1, 2]).json()["errors"]
    assert (error["field"], error["code"]) == ("(body)", "model_type")


def test_timestamp_window_comes_from_settings(settings: Settings, mutate) -> None:
    narrow = Settings(**{**settings.__dict__, "max_past_age": timedelta(seconds=5)})
    data = mutate(lambda d: d.update(timestamp=iso_z(datetime.now(UTC) - timedelta(minutes=1))))
    with TestClient(create_app(narrow)) as client:
        response = post(client, data)
    assert response.status_code == 422
    assert response.json()["errors"][0]["code"] == "timestamp_too_old"


# --- orden de comprobaciones -----------------------------------------------------


def test_order_401_before_415_413_400(client) -> None:
    assert post(client, "not json", key=None, headers={"Content-Type": "text/plain"}).status_code == 401


def test_order_415_before_413(client) -> None:
    response = post(client, _oversized(), headers={"Content-Type": "text/plain"})
    assert response.status_code == 415


def test_order_413_before_400(client) -> None:
    assert post(client, b"{" * 5000).status_code == 413


def test_order_422_before_403(client, mutate) -> None:
    data = mutate(lambda d: d["measurements"].update(relative_humidity_pct=101))
    assert post(client, data, key=KEYS["GDL-002"][0]).status_code == 422


def test_order_403_before_409(client, payload) -> None:
    assert post(client, payload).status_code == 201
    assert post(client, payload, key=KEYS["GDL-002"][0]).status_code == 403


# --- handler de RequestValidationError (rutas con cuerpo declarado) ---------------


def test_request_validation_handler_separates_400_and_422(settings: Settings) -> None:
    app = create_app(settings)

    @app.post("/_probe")
    def probe(reading: WeatherReading) -> dict:
        return {}

    with TestClient(app) as client:
        malformed = client.post(
            "/_probe", content='{\n  "a": }', headers={"Content-Type": "application/json"}
        )
        invalid = client.post("/_probe", json={"schema_version": "2.0"})
    assert malformed.status_code == 400
    assert malformed.headers["content-type"] == PROBLEM
    assert (malformed.json()["line"], malformed.json()["column"]) == (2, 8)
    assert invalid.status_code == 422
    assert {"field": "schema_version", "code": "literal_error"}.items() <= invalid.json()["errors"][0].items()


# --- 404 / 405 -------------------------------------------------------------------


def test_unknown_route_is_404_problem(client) -> None:
    response = client.post("/nope")
    assert response.status_code == 404
    assert response.headers["content-type"] == PROBLEM
    assert response.json() == {
        "type": "urn:skilliket:problem:not-found",
        "title": "Not Found",
        "status": 404,
        "detail": "Not Found",
        "instance": "/nope",
    }


def test_wrong_method_is_405_problem_with_allow(client) -> None:
    response = client.get(URL)
    assert response.status_code == 405
    assert response.headers["content-type"] == PROBLEM
    assert response.headers["allow"] == "POST"
    body = response.json()
    assert (body["type"], body["title"], body["status"], body["instance"]) == (
        "urn:skilliket:problem:method-not-allowed",
        "Method Not Allowed",
        405,
        URL,
    )


# --- 500, salud, documentación y logs ---------------------------------------------


class _BrokenStorage(StorageAdapter):
    def save(self, stored: StoredReading) -> None:
        raise OSError("disco lleno: /secret/path")


def test_unexpected_error_is_generic_500(settings: Settings, payload) -> None:
    app = create_app(settings, storage=_BrokenStorage())
    with TestClient(app, raise_server_exceptions=False) as client:
        response = post(client, payload)
    assert response.status_code == 500
    assert response.headers["content-type"] == PROBLEM
    assert response.json()["detail"] == "Error interno"
    assert "secret" not in response.text and "Traceback" not in response.text


def test_health_docs_and_openapi(client) -> None:
    assert client.get("/health").json() == {"status": "ok"}
    assert client.get("/docs").status_code == 200
    spec = client.get("/openapi.json").json()
    operation = spec["paths"][URL]["post"]
    assert operation["requestBody"]["content"]["application/json"]["schema"] == {
        "$ref": "#/components/schemas/WeatherReading"
    }
    assert {"WeatherReading", "Measurements", "Device", "Problem"} <= spec["components"]["schemas"].keys()
    assert set(operation["responses"]) >= {"201", "400", "401", "403", "409", "413", "415", "422"}


def test_logs_have_ids_and_never_the_key_or_payload(client, payload, log_records) -> None:
    key = KEYS["GDL-001"][0]
    post(client, payload, key=key)
    post(client, payload, key=key)
    post(client, payload, key="wrong-key")
    events = [r for r in log_records if getattr(r, "event", None) == "reading"]
    assert [r.status for r in events] == [201, 409, 401]
    assert events[0].reading_id and events[0].station_id == "GDL-001"
    for record in log_records:
        dumped = json.dumps(record.__dict__, default=str)
        assert key not in dumped and "wrong-key" not in dumped
        assert "841.3" not in dumped
```

=== ingest_api/tests/test_storage.py === (207 líneas, sha256: f30bf71b43a09e21bab62ecde099eb17900bfea1cc9f7b04c7dbac8f286491a4)

```python
import json
import threading
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from app.models import WeatherReading
from app.storage import DuplicateReadingError, JsonlStorage, StoredReading
from tests.conftest import FakeClock, make_payload

BASE = datetime.now(UTC).replace(microsecond=0) - timedelta(hours=1)
T0 = datetime(2026, 1, 10, tzinfo=UTC)
WEEK = timedelta(days=7)


def stored(offset_s: int = 0, station_id: str = "GDL-001") -> StoredReading:
    data = make_payload(station_id=station_id, timestamp=BASE + timedelta(seconds=offset_s))
    return StoredReading(
        reading_id=f"r-{offset_s}",
        received_at=datetime.now(UTC),
        reading=WeatherReading.model_validate_json(json.dumps(data)),
    )


def stored_at(ts: datetime, reading_id: str) -> StoredReading:
    """Lectura con timestamp arbitrario (la ventana se evalúa contra ese mismo instante)."""
    data = make_payload(timestamp=ts)
    reading = WeatherReading.model_validate_json(json.dumps(data), context={"now": ts})
    return StoredReading(reading_id=reading_id, received_at=ts, reading=reading)


def run_concurrently(targets) -> None:
    barrier = threading.Barrier(len(targets))

    def wrap(target):
        def run():
            barrier.wait()
            target()

        return run

    threads = [threading.Thread(target=wrap(t)) for t in targets]
    for t in threads:
        t.start()
    for t in threads:
        t.join()


def test_concurrent_distinct_saves_write_whole_lines(tmp_path: Path) -> None:
    storage = JsonlStorage(tmp_path / "r.jsonl")
    run_concurrently([lambda i=i: storage.save(stored(i)) for i in range(50)])
    storage.close()
    raw_lines = (tmp_path / "r.jsonl").read_text(encoding="utf-8").splitlines()
    records = [json.loads(line) for line in raw_lines]
    assert len(records) == 50
    assert len({r["reading_id"] for r in records}) == 50


def test_concurrent_same_reading_saves_once(tmp_path: Path) -> None:
    storage = JsonlStorage(tmp_path / "r.jsonl")
    outcomes: list[str] = []
    lock = threading.Lock()

    def attempt() -> None:
        try:
            storage.save(stored(0))
            result = "created"
        except DuplicateReadingError:
            result = "duplicate"
        with lock:
            outcomes.append(result)

    run_concurrently([attempt] * 20)
    storage.close()
    assert outcomes.count("created") == 1
    assert outcomes.count("duplicate") == 19


def test_index_rebuild_skips_corrupt_lines_and_keeps_appending_cleanly(tmp_path: Path) -> None:
    path = tmp_path / "r.jsonl"
    first = JsonlStorage(path)
    first.save(stored(0))
    first.close()
    with open(path, "ab") as f:
        f.write(b"not json\n")
        f.write(b'{"reading_id": "truncated", "reading": {"station_')  # sin salto de línea

    second = JsonlStorage(path)
    assert second.indexed_count == 1
    with pytest.raises(DuplicateReadingError):
        second.save(stored(0))
    second.save(stored(1))
    second.close()

    last = path.read_text(encoding="utf-8").splitlines()[-1]
    assert json.loads(last)["reading_id"] == "r-1"


class _HalfThenFail:
    """Envuelve el archivo: la primera escritura deja la mitad de los bytes y lanza OSError."""

    def __init__(self, real) -> None:
        self._real = real
        self.failed = False

    def write(self, data: bytes) -> int:
        if not self.failed:
            self.failed = True
            self._real.write(data[: len(data) // 2])
            self._real.flush()
            raise OSError("disco lleno (simulado)")
        return self._real.write(data)

    def __getattr__(self, name: str):
        return getattr(self._real, name)


def test_partial_write_failure_does_not_glue_next_reading(tmp_path: Path, log_records) -> None:
    path = tmp_path / "r.jsonl"
    storage = JsonlStorage(path)
    storage.save(stored(0))
    storage._file = _HalfThenFail(storage._file)
    with pytest.raises(OSError):
        storage.save(stored(1))
    storage.save(stored(2))
    storage.close()

    raw_lines = path.read_bytes().split(b"\n")
    assert raw_lines[-1] == b""
    assert json.loads(raw_lines[-2])["reading_id"] == "r-2"

    rebuilt = JsonlStorage(path)
    assert rebuilt.indexed_count == 2
    skipped = [r for r in log_records if getattr(r, "event", None) == "storage_line_skipped"]
    assert [r.line for r in skipped] == [2]
    rebuilt.save(stored(1))  # la lectura fallida nunca entró al índice
    with pytest.raises(DuplicateReadingError):
        rebuilt.save(stored(2))
    rebuilt.close()


def test_prune_drops_keys_outside_retention(tmp_path: Path) -> None:
    clock = FakeClock(T0)
    storage = JsonlStorage(tmp_path / "r.jsonl", max_past_age=WEEK, clock=clock)
    old_a = stored_at(T0 - timedelta(days=6), "old-a")
    old_b = stored_at(T0 - timedelta(days=6, hours=12), "old-b")
    recent = stored_at(T0 - timedelta(hours=1), "recent")
    for s in (old_a, old_b, recent):
        storage.save(s)

    # Retención = 7 días + 1 h; en T0 + 2 días el corte es T0 - 5 días - 1 h.
    clock.advance(days=2)
    before = storage.indexed_count
    storage.save(stored_at(clock.now - timedelta(minutes=1), "new"))
    after = storage.indexed_count
    assert (before, after) == (3, 2)

    with pytest.raises(DuplicateReadingError):
        storage.save(recent)
    storage.save(old_a)  # su clave se podó (la API ya la rechazaría con 422)
    storage.close()


def test_prune_runs_at_most_every_5_minutes(tmp_path: Path, monkeypatch) -> None:
    clock = FakeClock(T0)
    storage = JsonlStorage(tmp_path / "r.jsonl", max_past_age=WEEK, clock=clock)
    calls: list[datetime] = []
    original = storage._prune_locked

    def spy(now: datetime) -> None:
        calls.append(now)
        original(now)

    monkeypatch.setattr(storage, "_prune_locked", spy)
    for i in range(10):
        clock.advance(seconds=20)
        storage.save(stored_at(clock.now - timedelta(seconds=1), f"r{i}"))
    assert calls == []  # 200 s desde la creación
    clock.advance(minutes=2)
    storage.save(stored_at(clock.now - timedelta(seconds=1), "r10"))
    assert calls == [clock.now]
    storage.close()


def test_rebuild_loads_only_keys_inside_retention(tmp_path: Path) -> None:
    path = tmp_path / "r.jsonl"
    writer = JsonlStorage(path, max_past_age=timedelta(days=365), clock=FakeClock(T0))
    for i in range(200):
        writer.save(stored_at(T0 - timedelta(hours=i, minutes=30), f"r{i}"))
    writer.close()
    assert len(path.read_text(encoding="utf-8").splitlines()) == 200

    reader = JsonlStorage(path, max_past_age=timedelta(hours=100), clock=FakeClock(T0))
    # Retención = 100 h + 1 h: entran las de i h 30 min con i <= 100.
    assert reader.indexed_count == 101
    reader.close()


def test_record_format(tmp_path: Path) -> None:
    storage = JsonlStorage(tmp_path / "r.jsonl")
    storage.save(stored(0))
    storage.close()
    [record] = [json.loads(line) for line in (tmp_path / "r.jsonl").read_text().splitlines()]
    assert set(record) == {"reading_id", "received_at", "reading"}
    assert record["reading"]["timestamp"].endswith("Z")
    assert record["received_at"].endswith("Z")
```

=== ingest_api/tests/test_models.py === (144 líneas, sha256: 2022a55c11cd7b843d6c925da292285d177618508534cd98b2f875deb3e7a2a2)

```python
import json
import re
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from pydantic import ValidationError

from app.models import (
    STATION_ID_PATTERN,
    TIMESTAMP_PATTERN,
    WeatherReading,
    weather_reading_json_schema,
)
from tests.conftest import iso_z, make_payload

SCHEMA_FILE = Path(__file__).resolve().parents[2] / "schema" / "weather_reading.v1.schema.json"


def _errors(data: dict) -> list[tuple[str, str]]:
    with pytest.raises(ValidationError) as exc_info:
        WeatherReading.model_validate_json(json.dumps(data))
    return [(".".join(map(str, e["loc"])), e["type"]) for e in exc_info.value.errors()]


def test_exported_schema_matches_model() -> None:
    exported = json.loads(SCHEMA_FILE.read_text(encoding="utf-8"))
    assert exported == weather_reading_json_schema(), (
        "schema/weather_reading.v1.schema.json está desactualizado: "
        "ejecuta python scripts/export_schema.py"
    )


def test_valid_payload_parses() -> None:
    reading = WeatherReading.model_validate_json(json.dumps(make_payload()))
    assert reading.station_id == "GDL-001"
    assert reading.timestamp.tzinfo is not None


def test_minimal_payload_without_optional_fields() -> None:
    data = make_payload()
    del data["sequence"], data["device"]
    for field in ("wind_gust_ms", "wind_direction_deg", "solar_radiation_wm2"):
        del data["measurements"][field]
    WeatherReading.model_validate_json(json.dumps(data))


@pytest.mark.parametrize(
    ("speed", "direction"),
    [(0.0, None), (0.2, 180)],
    ids=["calm-null-direction", "low-speed-numeric-direction"],
)
def test_direction_is_not_tied_to_speed(speed: float, direction: int | None) -> None:
    data = make_payload()
    data["measurements"].update(wind_speed_ms=speed, wind_gust_ms=speed, wind_direction_deg=direction)
    WeatherReading.model_validate_json(json.dumps(data))


def test_gust_equal_to_speed_is_valid() -> None:
    data = make_payload()
    data["measurements"].update(wind_speed_ms=4.0, wind_gust_ms=4.0)
    WeatherReading.model_validate_json(json.dumps(data))


ARABIC_INDIC_STATION = "GDL-٠٠١"  # GDL-٠٠١
FULLWIDTH_TIMESTAMP = "２０２６-10-01T21:15:00Z"  # ２０２６-10-01T21:15:00Z


@pytest.mark.parametrize(
    ("pattern", "value"),
    [(STATION_ID_PATTERN, ARABIC_INDIC_STATION), (TIMESTAMP_PATTERN, FULLWIDTH_TIMESTAMP)],
    ids=["station-id", "timestamp"],
)
def test_patterns_reject_non_ascii_digits(pattern: str, value: str) -> None:
    # Python re trata \d como cualquier dígito Unicode; ECMAScript (JSON Schema), solo ASCII.
    assert re.fullmatch(pattern, value) is None


def _future(minutes: int) -> str:
    return iso_z(datetime.now(UTC) + timedelta(minutes=minutes))


def _past(days: int) -> str:
    return iso_z(datetime.now(UTC) - timedelta(days=days))


INVALID_CASES = [
    ("rh-101", lambda d: d["measurements"].update(relative_humidity_pct=101),
     "measurements.relative_humidity_pct", "less_than_equal"),
    ("temp-minus-41", lambda d: d["measurements"].update(air_temperature_c=-41),
     "measurements.air_temperature_c", "greater_than_equal"),
    ("gust-below-speed", lambda d: d["measurements"].update(wind_speed_ms=6.0, wind_gust_ms=5.0),
     "measurements.wind_gust_ms", "gust_less_than_speed"),
    ("extra-field", lambda d: d.update(humidity_extra=1), "humidity_extra", "extra_forbidden"),
    ("dew-point-present", lambda d: d["measurements"].update(dew_point_c=15.9),
     "measurements.dew_point_c", "extra_forbidden"),
    ("missing-required", lambda d: d["measurements"].pop("station_pressure_hpa"),
     "measurements.station_pressure_hpa", "missing"),
    ("timestamp-10min-future", lambda d: d.update(timestamp=_future(10)),
     "timestamp", "timestamp_in_future"),
    ("timestamp-8days-past", lambda d: d.update(timestamp=_past(8)), "timestamp", "timestamp_too_old"),
    ("timestamp-offset-not-z", lambda d: d.update(timestamp="2026-10-01T21:15:00+00:00"),
     "timestamp", "timestamp_format"),
    ("station-id-format", lambda d: d.update(station_id="gdl-1"), "station_id", "string_pattern_mismatch"),
    ("schema-version-2", lambda d: d.update(schema_version="2.0"), "schema_version", "literal_error"),
    ("string-in-number", lambda d: d["measurements"].update(air_temperature_c="24.7"),
     "measurements.air_temperature_c", "float_type"),
    ("bool-in-number", lambda d: d["measurements"].update(precipitation_mm=True),
     "measurements.precipitation_mm", "float_type"),
    ("interval-too-short", lambda d: d.update(interval_s=5), "interval_s", "greater_than_equal"),
    ("negative-sequence", lambda d: d.update(sequence=-1), "sequence", "greater_than_equal"),
    ("direction-360", lambda d: d["measurements"].update(wind_direction_deg=360),
     "measurements.wind_direction_deg", "less_than_equal"),
    ("device-extra", lambda d: d["device"].update(serial="x"), "device.serial", "extra_forbidden"),
    ("interval-float", lambda d: d.update(interval_s=60.0), "interval_s", "int_type"),
    ("sequence-float", lambda d: d.update(sequence=1.0), "sequence", "int_type"),
    ("station-id-unicode-digits", lambda d: d.update(station_id=ARABIC_INDIC_STATION),
     "station_id", "string_pattern_mismatch"),
    ("timestamp-fullwidth-digits", lambda d: d.update(timestamp=FULLWIDTH_TIMESTAMP),
     "timestamp", "timestamp_format"),
]


@pytest.mark.parametrize(
    ("change", "field", "code"),
    [case[1:] for case in INVALID_CASES],
    ids=[case[0] for case in INVALID_CASES],
)
def test_invalid_payloads(change, field: str, code: str) -> None:
    data = make_payload()
    change(data)
    assert (field, code) in _errors(data)


def test_window_uses_validation_context() -> None:
    data = make_payload(timestamp=datetime(2026, 1, 1, 12, 0, tzinfo=UTC))
    context = {
        "now": datetime(2026, 1, 1, 12, 0, 30, tzinfo=UTC),
        "max_future_skew": timedelta(seconds=10),
        "max_past_age": timedelta(seconds=20),
    }
    with pytest.raises(ValidationError) as exc_info:
        WeatherReading.model_validate_json(json.dumps(data), context=context)
    assert exc_info.value.errors()[0]["type"] == "timestamp_too_old"
```

=== docs/adr/0001-ingesta-y-persistencia.md === (75 líneas, sha256: 87f8250c2e84e5745618a503925e37152807c5776b3696b312336504592f0a3f)

```markdown
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
- **Escritura fallida a medias:** si `write`, `flush` o `fsync` fallan (p. ej. disco lleno), la respuesta es 500, la lectura no entra al índice y el archivo queda marcado como "sucio". La siguiente escritura exitosa antepone un salto de línea, así la lectura buena queda en su propia línea y no se pega a la parcial. La línea parcial se salta como corrupta al reconstruir. Si el que falló fue `fsync`, la línea pudo quedar completa en el archivo; como la clave no entró al índice, un reintento la vuelve a escribir. **La misma lectura puede quedar dos veces en el archivo**, aunque el índice la cuenta una sola vez. Quien consuma el JSONL debe deduplicar por `station_id` + `timestamp`.
- **Traceback del 500 en el log del servidor:** la respuesta al cliente es genérica (`"detail": "Error interno"`) y el log estructurado de la app solo registra el tipo de excepción. Pero Starlette vuelve a lanzar la excepción después de enviar el 500, así que uvicorn escribe el traceback completo en su log, **incluido el mensaje de la excepción** (verificado con `OSError("disco lleno: /secret/path")`). No se suprime porque sirve para depurar. En esa prueba, ni la clave ni los valores del payload aparecieron en el traceback; si una excepción futura incluyera datos en su mensaje, sí aparecerían.
- **Validación estricta de tipos:** `"24.7"` (string), `true` en un campo numérico y `60.0` en un campo entero (`interval_s`, `sequence`, `wind_direction_deg`) se rechazan con 422. El generador debe enviar enteros como enteros.
- **Ventana del timestamp:** depende del reloj del servidor. Se configura con `MAX_FUTURE_SKEW_S` y `MAX_PAST_AGE_S`.
```

=== .env.example === (16 líneas, sha256: 84728322dc910bac4731abf81f3be94e4542f4afbb854af7ea58639e6f4aae6b)

```dotenv
# Copia este archivo a .env y reemplaza los valores. Nunca pongas claves en claro aquí:
# solo hashes SHA-256 (hex, minúsculas) generados con scripts/gen_station_key.py.
#
# Ojo: ni uvicorn ni la app leen .env por sí solos; solo usan variables de entorno reales.
# Este archivo lo consumirá docker compose (Fase C). Para correr en local sin Docker,
# define las variables en la terminal (ver README).

# JSON {station_id: [hash_clave_1, hash_clave_2]}. Una o dos claves por estación (rotación).
STATION_KEY_HASHES={"GDL-001": ["<sha256-hex-clave-primaria>", "<sha256-hex-clave-secundaria>"]}

# Archivo JSON Lines donde se guarda cada lectura validada.
STORAGE_PATH=./data/readings.jsonl

# Ventana de aceptación del timestamp, en segundos.
MAX_FUTURE_SKEW_S=300
MAX_PAST_AGE_S=604800
```

