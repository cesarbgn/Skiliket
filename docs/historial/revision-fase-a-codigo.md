# Revisión Fase A — código

Volcado literal generado por script desde los bytes de cada archivo. Cada bloque está delimitado por una cerca de backticks más larga que cualquier secuencia de backticks del archivo. N = líneas según `wc -l` (saltos de línea); sha256 = hash de los bytes del archivo.

=== ingest_api/app/auth.py === (33 líneas, sha256: f06535c55c3dd9bbd989869af31eb3063b2ecddb7f61d64506aed4c72a4ac544)

```python
"""Autenticación de estaciones por API key (cabecera X-Station-Key)."""

import hashlib
import hmac
from collections.abc import Mapping, Sequence

STATION_KEY_HEADER = "X-Station-Key"


def hash_key(key: str) -> str:
    """SHA-256 en hex minúsculas; es lo único que se configura y se guarda."""
    return hashlib.sha256(key.encode("utf-8")).hexdigest()


class KeyVerifier:
    def __init__(self, station_key_hashes: Mapping[str, Sequence[str]]) -> None:
        self._entries = [
            (station_id, digest.encode("ascii"))
            for station_id, digests in station_key_hashes.items()
            for digest in digests
        ]

    def station_for_key(self, key: str | None) -> str | None:
        """Devuelve la estación dueña de la clave, o None si no es válida."""
        if not key:
            return None
        candidate = hash_key(key).encode("ascii")
        match = None
        # Se recorren todas las entradas (sin salir antes) para no filtrar por tiempo cuál coincidió.
        for station_id, digest in self._entries:
            if hmac.compare_digest(candidate, digest):
                match = station_id
        return match
```

=== ingest_api/app/main.py === (257 líneas, sha256: 1c8d13b2fadc7255b07974576772f488732ea157dc2525e59ef976a5f397ee34)

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
    if declared is not None and declared.isdigit() and int(declared) > limit:
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
    store = storage or JsonlStorage(settings.storage_path)

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

=== ingest_api/app/errors.py === (133 líneas, sha256: fc7e47368f469fc67794b86e1c59a100ca6507ed26dad89bbf836b5a29c24af8)

```python
"""Respuestas de error RFC 9457 (application/problem+json)."""

import logging
from collections.abc import Iterable, Sequence
from datetime import datetime
from typing import Any

from fastapi import Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel

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
    return JSONResponse(body, status_code=status, media_type=PROBLEM_MEDIA_TYPE)


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


async def unhandled_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    # Solo el tipo de excepción: el mensaje podría contener datos del payload.
    logger.error(
        "unhandled_error",
        extra={"event": "unhandled_error", "status": 500, "reason": type(exc).__name__},
    )
    return problem(500, "internal-error", "Internal Server Error", "Error interno", request.url.path)
```

=== ingest_api/app/storage.py === (116 líneas, sha256: a84c3e68cc8d201d75f8480f385594f0b17201cd3e213aabc61123a8d3a93c8c)

```python
"""Persistencia de lecturas validadas."""

import json
import logging
import os
import threading
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from app.models import WeatherReading

logger = logging.getLogger("skilliket.ingest.storage")

DedupKey = tuple[str, datetime]


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
    arrancar. Válido solo para un proceso: varios workers no comparten el lock ni el índice."""

    def __init__(self, path: Path) -> None:
        self._path = path
        self._lock = threading.Lock()
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
        with open(self._path, "rb") as f:
            for lineno, line in enumerate(f, start=1):
                if not line.strip():
                    continue
                try:
                    reading = json.loads(line)["reading"]
                    index.add(
                        (reading["station_id"], datetime.fromisoformat(reading["timestamp"]))
                    )
                except (ValueError, KeyError, TypeError):
                    logger.warning(
                        "storage_line_skipped", extra={"event": "storage_line_skipped", "line": lineno}
                    )
        return index

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
            if stored.dedup_key in self._index:
                raise DuplicateReadingError
            # Una sola escritura por línea completa bajo el lock: sin intercalado entre hilos.
            self._file.write(data)
            self._file.flush()
            os.fsync(self._file.fileno())
            self._index.add(stored.dedup_key)

    def close(self) -> None:
        with self._lock:
            if not self._file.closed:
                self._file.close()
```

=== ingest_api/app/models.py === (132 líneas, sha256: 8ce6fb2e2e00cf6cf20657d75913c1e379037e768d696c19eb7934af30b1f913)

```python
"""Contrato v1 de una lectura meteorológica (fuente única de verdad del esquema)."""

import re
from datetime import UTC, datetime, timedelta
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, field_validator
from pydantic_core import PydanticCustomError

SCHEMA_VERSION = "1.0"
STATION_ID_PATTERN = r"^[A-Z]{3}-\d{3}$"
TIMESTAMP_PATTERN = r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,6})?Z$"
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

=== ingest_api/app/config.py === (86 líneas, sha256: 835a4ed902c2048e0d09528fd5366f2c0bfb13e22ef932b292f2f713f2648d8f)

```python
"""Configuración del API leída de variables de entorno."""

import json
import os
import re
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path

from app.models import DEFAULT_MAX_FUTURE_SKEW, DEFAULT_MAX_PAST_AGE, STATION_ID_PATTERN

DEFAULT_STORAGE_PATH = "./data/readings.jsonl"
DEFAULT_MAX_FUTURE_SKEW_S = int(DEFAULT_MAX_FUTURE_SKEW.total_seconds())
DEFAULT_MAX_PAST_AGE_S = int(DEFAULT_MAX_PAST_AGE.total_seconds())
MAX_BODY_BYTES = 4096
MAX_KEYS_PER_STATION = 2

_SHA256_HEX = re.compile(r"^[0-9a-f]{64}$")
_STATION_ID = re.compile(STATION_ID_PATTERN)


class ConfigError(ValueError):
    """Configuración inválida; el API no debe arrancar."""


@dataclass(frozen=True)
class Settings:
    station_key_hashes: dict[str, tuple[str, ...]]
    storage_path: Path
    max_future_skew: timedelta
    max_past_age: timedelta
    max_body_bytes: int = MAX_BODY_BYTES


def parse_station_key_hashes(raw: str) -> dict[str, tuple[str, ...]]:
    """Valida STATION_KEY_HASHES: JSON {station_id: [hash1, hash2]}."""
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ConfigError(f"STATION_KEY_HASHES no es JSON válido: {exc.msg}") from None
    if not isinstance(data, dict) or not data:
        raise ConfigError("STATION_KEY_HASHES debe ser un objeto JSON no vacío")

    result: dict[str, tuple[str, ...]] = {}
    seen: set[str] = set()
    for station_id, hashes in data.items():
        if not _STATION_ID.fullmatch(station_id):
            raise ConfigError(f"station_id inválido en STATION_KEY_HASHES: {station_id!r}")
        if not isinstance(hashes, list) or not 1 <= len(hashes) <= MAX_KEYS_PER_STATION:
            raise ConfigError(
                f"{station_id}: se esperan entre 1 y {MAX_KEYS_PER_STATION} hashes en una lista"
            )
        for h in hashes:
            if not isinstance(h, str) or not _SHA256_HEX.fullmatch(h):
                raise ConfigError(f"{station_id}: cada hash debe ser SHA-256 en hex minúsculas")
        if len(set(hashes)) != len(hashes) or seen.intersection(hashes):
            # Un hash compartido haría ambigua la estación autenticada.
            raise ConfigError(f"{station_id}: hash repetido (dentro de la estación o con otra)")
        seen.update(hashes)
        result[station_id] = tuple(hashes)
    return result


def _int_env(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    try:
        value = int(raw)
    except ValueError:
        raise ConfigError(f"{name} debe ser un entero") from None
    if value < 0:
        raise ConfigError(f"{name} no puede ser negativo")
    return value


def load_settings() -> Settings:
    raw_hashes = os.environ.get("STATION_KEY_HASHES")
    if not raw_hashes:
        raise ConfigError("Falta STATION_KEY_HASHES; el API no acepta lecturas sin claves")
    return Settings(
        station_key_hashes=parse_station_key_hashes(raw_hashes),
        storage_path=Path(os.environ.get("STORAGE_PATH") or DEFAULT_STORAGE_PATH),
        max_future_skew=timedelta(seconds=_int_env("MAX_FUTURE_SKEW_S", DEFAULT_MAX_FUTURE_SKEW_S)),
        max_past_age=timedelta(seconds=_int_env("MAX_PAST_AGE_S", DEFAULT_MAX_PAST_AGE_S)),
    )
```

=== ingest_api/app/log.py === (35 líneas, sha256: 1f5a4bd94367dd2f27ae2763e91d30ddb64094c98fbb3974e813cecd49cf2147)

```python
"""Logs estructurados (una línea JSON por evento). Nunca incluyen claves ni payloads."""

import json
import logging
import sys
from datetime import UTC, datetime

_EXTRA_FIELDS = ("event", "reading_id", "station_id", "status", "reason", "codes", "line")


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": datetime.fromtimestamp(record.created, UTC).isoformat().replace("+00:00", "Z"),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        for name in _EXTRA_FIELDS:
            if hasattr(record, name):
                payload[name] = getattr(record, name)
        # ASCII escapado: la consola de Windows puede ser cp1252.
        return json.dumps(payload)


def configure_logging() -> None:
    logger = logging.getLogger("skilliket")
    if any(getattr(h, "_skilliket", False) for h in logger.handlers):
        return
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    handler._skilliket = True  # type: ignore[attr-defined]
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False
```

=== ingest_api/app/__init__.py === (1 líneas, sha256: fdcd1457eab48220efc88e0e1fcb908b0e4675b16430cc9b4b703eb039d21007)

```python
"""API de ingesta de lecturas meteorológicas de Skilliket."""
```

=== scripts/gen_station_key.py === (23 líneas, sha256: e66d7a6aea405b3c936d74a1218032c0e08156f3b51c7eb84ca94793c64e43a0)

```python
"""Genera una clave de estación: 32 bytes aleatorios en base64 urlsafe.

Imprime la clave en claro UNA sola vez y su hash SHA-256. Solo el hash va en
STATION_KEY_HASHES; la clave se entrega a la estación y no se guarda en ningún archivo.

Uso: python scripts/gen_station_key.py
"""

import hashlib
import secrets
import sys


def main() -> None:
    key = secrets.token_urlsafe(32)
    digest = hashlib.sha256(key.encode("utf-8")).hexdigest()
    print(f"STATION_KEY={key}")
    print(f"SHA256={digest}")
    print("Copia la clave ahora: no se vuelve a mostrar ni se guarda.", file=sys.stderr)


if __name__ == "__main__":
    main()
```

=== scripts/export_schema.py === (29 líneas, sha256: e6b5db0a2c0a1b3599a6ac59533083af40b049272c98d6b25f5f72bfadacd190)

```python
"""Regenera schema/weather_reading.v1.schema.json desde el modelo Pydantic.

Uso: python scripts/export_schema.py
"""

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = ROOT / "schema" / "weather_reading.v1.schema.json"

sys.path.insert(0, str(ROOT / "ingest_api"))

from app.models import weather_reading_json_schema  # noqa: E402


def render() -> str:
    return json.dumps(weather_reading_json_schema(), indent=2, ensure_ascii=False) + "\n"


def main() -> None:
    SCHEMA_PATH.parent.mkdir(parents=True, exist_ok=True)
    SCHEMA_PATH.write_text(render(), encoding="utf-8", newline="\n")
    print(f"Escrito {SCHEMA_PATH.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
```

=== schema/weather_reading.v1.schema.json === (206 líneas, sha256: e43a566454b92bfcfd408ce8303d247101ac5185df8bea1a1d20ef3c0d787478)

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
      "pattern": "^[A-Z]{3}-\\d{3}$",
      "title": "Station Id",
      "type": "string"
    },
    "timestamp": {
      "description": "Instante de la medición, RFC 3339 en UTC con sufijo Z",
      "examples": [
        "2026-10-01T21:15:00Z"
      ],
      "format": "date-time",
      "pattern": "^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(\\.\\d{1,6})?Z$",
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

