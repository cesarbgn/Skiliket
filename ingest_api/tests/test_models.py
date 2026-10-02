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
