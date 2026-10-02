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
