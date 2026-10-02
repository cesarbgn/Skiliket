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
