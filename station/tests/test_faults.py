import copy
import json
from collections import Counter
from datetime import UTC, datetime, timedelta

import pytest

from station.faults import BY_NAME, CATALOG, Fault, FaultInjector, fault_seed
from station.weather_model import ReadingGenerator, Site

T0 = datetime(2026, 10, 1, 6, 0, tzinfo=UTC)
NOW = datetime(2026, 10, 1, 7, 0, tzinfo=UTC)


def readings(n: int, seed: int = 3) -> list[dict]:
    generator = ReadingGenerator("GDL-001", 60, seed, Site())
    return [generator.next(T0 + timedelta(minutes=i)) for i in range(n)]


def test_catalog_has_the_nine_agreed_faults() -> None:
    assert [(f.name, f.field, f.code) for f in CATALOG] == [
        ("humidity_over_100", "measurements.relative_humidity_pct", "less_than_equal"),
        ("temperature_below_min", "measurements.air_temperature_c", "greater_than_equal"),
        ("gust_below_speed", "measurements.wind_gust_ms", "gust_less_than_speed"),
        ("extra_field", "unexpected_field", "extra_forbidden"),
        ("missing_required", "measurements.station_pressure_hpa", "missing"),
        ("string_in_number", "measurements.air_temperature_c", "float_type"),
        ("future_timestamp", "timestamp", "timestamp_in_future"),
        ("bad_schema_version", "schema_version", "literal_error"),
        ("float_in_integer", "interval_s", "int_type"),
    ]


def test_rate_1_mutates_every_reading_and_covers_every_fault() -> None:
    injector = FaultInjector(1.0, seed=11)
    seen = Counter()
    for payload in readings(300):
        mutated, fault = injector.maybe_inject(payload, NOW)
        # Se compara el JSON: en Python 60.0 == 60, pero el API los distingue.
        assert fault is not None and json.dumps(mutated) != json.dumps(payload)
        seen[fault.name] += 1
    assert set(seen) == set(BY_NAME)


def test_rate_0_never_mutates() -> None:
    injector = FaultInjector(0.0, seed=11)
    pool = readings(200)
    for i in range(2000):
        payload = pool[i % len(pool)]
        original = copy.deepcopy(payload)
        mutated, fault = injector.maybe_inject(payload, NOW)
        assert fault is None and mutated is payload and payload == original


def test_rate_0_3_proportion_over_2000() -> None:
    injector = FaultInjector(0.3, seed=5)
    payload = readings(1)[0]
    hits = sum(injector.maybe_inject(payload, NOW)[1] is not None for _ in range(2000))
    # Binomial(2000, 0.3): media 600, desviación ~20.5; ±4 desviaciones.
    assert 518 <= hits <= 682


def test_mutation_does_not_touch_original() -> None:
    injector = FaultInjector(1.0, seed=2)
    payload = readings(1)[0]
    original = copy.deepcopy(payload)
    for _ in range(50):
        injector.maybe_inject(payload, NOW)
    assert payload == original


@pytest.mark.parametrize("fault", CATALOG, ids=lambda f: f.name)
def test_each_mutation(fault: Fault) -> None:
    payload = copy.deepcopy(readings(1)[0])
    payload["measurements"]["wind_speed_ms"] = 0.3
    fault.apply(payload, NOW)
    m = payload["measurements"]
    match fault.name:
        case "humidity_over_100":
            assert m["relative_humidity_pct"] == 101
        case "temperature_below_min":
            assert m["air_temperature_c"] == -41
        case "gust_below_speed":
            assert m["wind_speed_ms"] >= 1 and m["wind_gust_ms"] < m["wind_speed_ms"]
        case "extra_field":
            assert "unexpected_field" in payload
        case "missing_required":
            assert "station_pressure_hpa" not in m
        case "string_in_number":
            assert isinstance(m["air_temperature_c"], str)
        case "future_timestamp":
            assert payload["timestamp"] == "2026-10-01T07:10:00Z"
        case "bad_schema_version":
            assert payload["schema_version"] == "2.0"
        case "float_in_integer":
            assert type(payload["interval_s"]) is float
            assert '"interval_s": 60.0' in json.dumps(payload)


def test_fault_rng_is_independent_from_weather_seed() -> None:
    assert fault_seed(1) != 1 and fault_seed(1) == fault_seed(1) and fault_seed(1) != fault_seed(2)


def test_same_seed_same_faults() -> None:
    a, b = FaultInjector(0.3, seed=9), FaultInjector(0.3, seed=9)
    payload = readings(1)[0]
    names_a = [getattr(a.maybe_inject(payload, NOW)[1], "name", None) for _ in range(200)]
    names_b = [getattr(b.maybe_inject(payload, NOW)[1], "name", None) for _ in range(200)]
    assert names_a == names_b
