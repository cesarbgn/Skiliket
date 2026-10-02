"""Configuración de la estación leída de variables de entorno (fail-fast).

Los mensajes de error nunca incluyen valores: así una clave pegada por error en otra
variable tampoco termina en la salida."""

import hashlib
import math
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from urllib.parse import urlsplit

STATION_ID_PATTERN = r"^[A-Z]{3}-[0-9]{3}$"
# ASCII visible sin espacios: es seguro como valor de cabecera HTTP.
_KEY_PATTERN = r"^[\x21-\x7e]{16,256}$"

DEFAULT_INGEST_URL = "http://127.0.0.1:8000/v1/readings"
INTERVAL_MIN_S, INTERVAL_MAX_S = 10, 3600  # mismos límites que interval_s en el API
# El API rechaza timestamps de más de 7 días; 167 h deja 1 h de margen para enviar la réplica.
REPLAY_MAX_HOURS = 167

_STATION_ID = re.compile(STATION_ID_PATTERN)
_KEY = re.compile(_KEY_PATTERN)


class ConfigError(ValueError):
    """Configuración inválida; la estación no debe arrancar."""


class Secret:
    """Envuelve la clave para que repr(), str() y f-strings no la muestren."""

    __slots__ = ("_value",)

    def __init__(self, value: str) -> None:
        self._value = value

    def reveal(self) -> str:
        return self._value

    def __repr__(self) -> str:
        return "Secret('***')"

    __str__ = __repr__

    def __eq__(self, other: object) -> bool:
        return isinstance(other, Secret) and other._value == self._value

    def __hash__(self) -> int:
        return hash(self._value)


@dataclass(frozen=True)
class Config:
    station_id: str
    station_key: Secret = field(repr=False)
    ingest_url: str = DEFAULT_INGEST_URL
    interval_s: int = 60
    seed: int = 0
    lat: float = 20.67
    lon: float = -103.35
    elevation_m: float = 1550.0
    tz_offset_h: float = -6.0
    prevailing_wind_deg: int = 250
    replay_hours: int = 0
    replay_duration_min: float = 2.0
    fault_rate: float = 0.0
    buffer_max: int = 1000
    max_readings: int = 0
    http_connect_timeout_s: float = 5.0
    http_read_timeout_s: float = 10.0


def default_seed(station_id: str) -> int:
    """Semilla estable entre ejecuciones y plataformas (hash() de str cambia por proceso)."""
    return int.from_bytes(hashlib.sha256(station_id.encode("ascii")).digest()[:8], "big")


def _get(env: Mapping[str, str], name: str) -> str | None:
    raw = env.get(name)
    if raw is None or raw.strip() == "":
        return None
    return raw.strip()


def _int(env: Mapping[str, str], name: str, default: int, lo: int, hi: int) -> int:
    raw = _get(env, name)
    if raw is None:
        return default
    try:
        value = int(raw)
    except ValueError:
        raise ConfigError(f"{name} debe ser un entero entre {lo} y {hi}") from None
    if not lo <= value <= hi:
        raise ConfigError(f"{name} debe ser un entero entre {lo} y {hi}")
    return value


def _float(env: Mapping[str, str], name: str, default: float, lo: float, hi: float) -> float:
    raw = _get(env, name)
    if raw is None:
        return default
    try:
        value = float(raw)
    except ValueError:
        raise ConfigError(f"{name} debe ser un número entre {lo} y {hi}") from None
    if not math.isfinite(value) or not lo <= value <= hi:
        raise ConfigError(f"{name} debe ser un número entre {lo} y {hi}")
    return value


def _timeout(env: Mapping[str, str], name: str, default: float) -> float:
    value = _float(env, name, default, 0.0, 120.0)
    if value <= 0:
        raise ConfigError(f"{name} debe ser mayor que 0 y como máximo 120")
    return value


def _url(env: Mapping[str, str]) -> str:
    raw = _get(env, "INGEST_URL")
    if raw is None:
        return DEFAULT_INGEST_URL
    try:
        parts = urlsplit(raw)
        _ = parts.port  # lanza ValueError si el puerto no es válido
    except ValueError:
        raise ConfigError("INGEST_URL no es una URL válida") from None
    if parts.scheme not in ("http", "https") or not parts.hostname:
        raise ConfigError("INGEST_URL debe ser una URL http(s) con host")
    if parts.username or parts.password:
        raise ConfigError("INGEST_URL no debe llevar credenciales; la clave va en STATION_KEY")
    return raw


def load_config(env: Mapping[str, str]) -> Config:
    station_id = _get(env, "STATION_ID")
    if station_id is None:
        raise ConfigError("Falta STATION_ID (formato AAA-000, p. ej. GDL-001)")
    if not _STATION_ID.fullmatch(station_id):
        raise ConfigError("STATION_ID debe cumplir ^[A-Z]{3}-[0-9]{3}$ (p. ej. GDL-001)")

    key = env.get("STATION_KEY")
    if not key:
        raise ConfigError("Falta STATION_KEY (genérala con scripts/gen_station_key.py)")
    if not _KEY.fullmatch(key):
        raise ConfigError(
            "STATION_KEY debe tener entre 16 y 256 caracteres ASCII visibles, sin espacios"
        )

    return Config(
        station_id=station_id,
        station_key=Secret(key),
        ingest_url=_url(env),
        interval_s=_int(env, "INTERVAL_S", 60, INTERVAL_MIN_S, INTERVAL_MAX_S),
        seed=_int(env, "SEED", default_seed(station_id), 0, 2**64 - 1),
        lat=_float(env, "LAT", 20.67, -90.0, 90.0),
        lon=_float(env, "LON", -103.35, -180.0, 180.0),
        # 5,000 m da ~540 hPa: dentro del rango 500–1100 que acepta el API.
        elevation_m=_float(env, "ELEVATION_M", 1550.0, -400.0, 5000.0),
        tz_offset_h=_float(env, "TZ_OFFSET_H", -6.0, -12.0, 14.0),
        prevailing_wind_deg=_int(env, "PREVAILING_WIND_DEG", 250, 0, 359),
        replay_hours=_int(env, "REPLAY_HOURS", 0, 0, REPLAY_MAX_HOURS),
        replay_duration_min=_float(env, "REPLAY_DURATION_MIN", 2.0, 0.0, 1440.0),
        fault_rate=_float(env, "FAULT_RATE", 0.0, 0.0, 1.0),
        buffer_max=_int(env, "BUFFER_MAX", 1000, 1, 1_000_000),
        max_readings=_int(env, "MAX_READINGS", 0, 0, 10_000_000),
        http_connect_timeout_s=_timeout(env, "HTTP_CONNECT_TIMEOUT_S", 5.0),
        http_read_timeout_s=_timeout(env, "HTTP_READ_TIMEOUT_S", 10.0),
    )
