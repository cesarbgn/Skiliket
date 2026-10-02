"""La salida del generador cumple el modelo REAL del API, y cada falla del catálogo
falla contra ese modelo con exactamente el (field, code) esperado.

24 h x 3 estaciones a 60 s = 4,320 payloads, serializados como los envía el emisor
(json.dumps compacto) y validados con el contexto "now" igual al timestamp de cada uno."""

import copy
import json
from datetime import UTC, datetime, timedelta
from functools import lru_cache
from typing import Any

import pytest
from pydantic import ValidationError

from app.errors import field_errors
from app.models import WeatherReading
from station.config import default_seed
from station.faults import CATALOG, Fault, FaultInjector
from station.weather_model import ReadingGenerator, Site

STATIONS = ("GDL-001", "GDL-002", "GDL-003")
INTERVAL_S = 60
START = datetime(2026, 10, 1, 6, 0, tzinfo=UTC)  # 00:00 hora local (UTC-6)


@lru_cache(maxsize=None)
def readings(station_id: str) -> tuple[tuple[datetime, dict[str, Any]], ...]:
    generator = ReadingGenerator(station_id, INTERVAL_S, default_seed(station_id), Site())
    out = []
    for k in range(86400 // INTERVAL_S):
        ts = START + timedelta(seconds=k * INTERVAL_S)
        out.append((ts, generator.next(ts)))
    return tuple(out)


def serialize(payload: dict[str, Any]) -> str:
    return json.dumps(payload, separators=(",", ":"))  # igual que station.sender.Sender


def validation_pairs(payload: dict[str, Any], now: datetime) -> set[tuple[str, str]]:
    """Pares (field, code) que devolvería el API en el 422; vacío si es válido."""
    try:
        WeatherReading.model_validate_json(serialize(payload), context={"now": now})
    except ValidationError as exc:
        return {(e["field"], e["code"]) for e in field_errors(exc.errors())}
    return set()


def test_all_4320_payloads_are_valid() -> None:
    total = valid = 0
    failures = []
    for station_id in STATIONS:
        for ts, payload in readings(station_id):
            total += 1
            pairs = validation_pairs(payload, ts)
            if pairs:
                failures.append((station_id, payload["sequence"], sorted(pairs)))
            else:
                valid += 1
    assert failures == []
    assert (total, valid) == (4320, 4320)


def test_stations_produce_different_series() -> None:
    temps = {
        s: tuple(p["measurements"]["air_temperature_c"] for _, p in readings(s)) for s in STATIONS
    }
    assert len(set(temps.values())) == 3


@pytest.mark.parametrize("fault", CATALOG, ids=lambda f: f.name)
def test_each_fault_fails_with_expected_field_and_code(fault: Fault) -> None:
    # Muestras a lo largo del día de las tres estaciones, incluidas lecturas en calma
    # (wind_direction_deg null), sobre las que gust_below_speed sube la velocidad a >= 1.
    checked = calm = 0
    for station_id in STATIONS:
        for ts, payload in readings(station_id)[::37]:
            mutated = copy.deepcopy(payload)
            fault.apply(mutated, ts)
            assert validation_pairs(mutated, ts) == {(fault.field, fault.code)}, payload["sequence"]
            checked += 1
            calm += payload["measurements"]["wind_direction_deg"] is None
    assert checked >= 100 and calm > 0


def test_injector_rate_1_over_all_payloads_matches_catalog() -> None:
    seen = set()
    for station_id in STATIONS:
        injector = FaultInjector(1.0, default_seed(station_id))
        for ts, payload in readings(station_id):
            mutated, fault = injector.maybe_inject(payload, ts)
            assert fault is not None
            assert validation_pairs(mutated, ts) == {(fault.field, fault.code)}
            seen.add(fault.name)
    assert seen == {f.name for f in CATALOG}
