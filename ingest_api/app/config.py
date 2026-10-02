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
