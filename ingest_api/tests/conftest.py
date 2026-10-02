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
