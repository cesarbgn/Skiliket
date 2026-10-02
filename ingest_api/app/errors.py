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
