# Revisión Fase A — pruebas y documentación

Volcado literal generado por script desde los bytes de cada archivo. Cada bloque está delimitado por una cerca de backticks más larga que cualquier secuencia de backticks del archivo. N = líneas según `wc -l` (saltos de línea); sha256 = hash de los bytes del archivo.

=== ingest_api/tests/conftest.py === (105 líneas, sha256: 27dc298ff867efe1ced0631fafe8a414837cf1c352d1aa6592303bb3df90c38d)

```python
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
```

=== ingest_api/tests/test_auth.py === (71 líneas, sha256: daf52055e1f70fe7071db862ec0ef1b9ee250eb7c54f63357d83060765dd5913)

```python
import json

import pytest

from app.auth import KeyVerifier, hash_key
from app.config import ConfigError, load_settings, parse_station_key_hashes

H1, H2, H3 = (hash_key(k) for k in ("a", "b", "c"))


def test_both_station_keys_resolve_to_station() -> None:
    verifier = KeyVerifier({"GDL-001": (hash_key("k1"), hash_key("k2")), "GDL-002": (hash_key("k3"),)})
    assert verifier.station_for_key("k1") == "GDL-001"
    assert verifier.station_for_key("k2") == "GDL-001"
    assert verifier.station_for_key("k3") == "GDL-002"


@pytest.mark.parametrize("key", [None, "", "k4"])
def test_unknown_or_missing_key_is_rejected(key) -> None:
    assert KeyVerifier({"GDL-001": (hash_key("k1"),)}).station_for_key(key) is None


def test_wrong_hash_rejects_its_key() -> None:
    wrong = hash_key("k1")[:-1] + ("0" if hash_key("k1")[-1] != "0" else "1")
    assert KeyVerifier({"GDL-001": (wrong,)}).station_for_key("k1") is None


def test_parse_valid_hashes() -> None:
    assert parse_station_key_hashes(json.dumps({"GDL-001": [H1, H2]})) == {"GDL-001": (H1, H2)}


@pytest.mark.parametrize(
    "raw",
    [
        "not json",
        "{}",
        "[]",
        json.dumps({"gdl-001": [H1]}),
        json.dumps({"GDL-001": []}),
        json.dumps({"GDL-001": [H1, H2, H3]}),
        json.dumps({"GDL-001": [H1.upper()]}),
        json.dumps({"GDL-001": ["abc"]}),
        json.dumps({"GDL-001": [H1, H1]}),
        json.dumps({"GDL-001": [H1], "GDL-002": [H1]}),
        json.dumps({"GDL-001": H1}),
    ],
    ids=[
        "not-json", "empty", "not-object", "bad-station-id", "no-hashes", "three-hashes",
        "uppercase-hex", "short-hash", "repeated-in-station", "shared-across-stations", "not-list",
    ],
)
def test_invalid_hash_config_is_rejected(raw: str) -> None:
    with pytest.raises(ConfigError):
        parse_station_key_hashes(raw)


def test_settings_require_station_hashes(monkeypatch) -> None:
    monkeypatch.delenv("STATION_KEY_HASHES", raising=False)
    with pytest.raises(ConfigError):
        load_settings()


def test_settings_from_env(monkeypatch) -> None:
    monkeypatch.setenv("STATION_KEY_HASHES", json.dumps({"GDL-001": [H1]}))
    monkeypatch.setenv("MAX_FUTURE_SKEW_S", "60")
    monkeypatch.delenv("STORAGE_PATH", raising=False)
    monkeypatch.delenv("MAX_PAST_AGE_S", raising=False)
    settings = load_settings()
    assert settings.max_future_skew.total_seconds() == 60
    assert settings.max_past_age.total_seconds() == 7 * 24 * 3600
    assert settings.storage_path.as_posix() == "data/readings.jsonl"
```

=== ingest_api/tests/test_readings.py === (306 líneas, sha256: fb66b00852bd0cdb9846a07fc60fd2d79c94ac8d6dd5d11395983a57f5dcb753)

```python
import json
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta

import pytest
from fastapi.testclient import TestClient

from app.config import Settings
from app.main import create_app
from app.models import WeatherReading
from app.storage import StorageAdapter, StoredReading
from tests.conftest import KEYS, iso_z, make_payload

URL = "/v1/readings"
PROBLEM = "application/problem+json"


def post(client: TestClient, body: object, key: str | None = KEYS["GDL-001"][0], **kwargs):
    headers = {"Content-Type": "application/json", **kwargs.pop("headers", {})}
    if key is not None:
        headers["X-Station-Key"] = key
    content = body if isinstance(body, (bytes, str)) else json.dumps(body)
    return client.post(URL, content=content, headers=headers, **kwargs)


def lines(path) -> list[dict]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line]


# --- 201 / 409 -------------------------------------------------------------------


def test_valid_payload_is_created_and_persisted(client, payload, storage_path) -> None:
    response = post(client, payload)
    assert response.status_code == 201
    body = response.json()
    assert body["status"] == "created"
    assert body["station_id"] == "GDL-001"
    assert body["received_at"].endswith("Z")
    [record] = lines(storage_path)
    assert record["reading_id"] == body["reading_id"]
    assert record["reading"] == payload


def test_duplicate_is_409(client, payload, storage_path) -> None:
    assert post(client, payload).status_code == 201
    response = post(client, payload)
    assert response.status_code == 409
    assert response.headers["content-type"] == PROBLEM
    assert len(lines(storage_path)) == 1


def test_duplicate_detection_uses_instant_not_text(client, payload) -> None:
    assert post(client, payload).status_code == 201
    payload["timestamp"] = payload["timestamp"].replace("Z", ".000Z")
    assert post(client, payload).status_code == 409


def test_index_is_rebuilt_after_restart(settings: Settings, payload) -> None:
    with TestClient(create_app(settings)) as first:
        assert post(first, payload).status_code == 201
    with TestClient(create_app(settings)) as second:
        assert post(second, payload).status_code == 409


@pytest.mark.parametrize(
    ("speed", "direction"), [(0.0, None), (0.2, 90)], ids=["null-direction", "numeric-direction"]
)
def test_low_wind_direction_cases_are_created(client, mutate, speed, direction) -> None:
    data = mutate(
        lambda d: d["measurements"].update(
            wind_speed_ms=speed, wind_gust_ms=speed, wind_direction_deg=direction
        )
    )
    assert post(client, data).status_code == 201


# --- 401 / 403 -------------------------------------------------------------------


def test_missing_key_is_401(client, payload) -> None:
    response = post(client, payload, key=None)
    assert response.status_code == 401
    assert response.headers["content-type"] == PROBLEM
    assert response.json()["status"] == 401


def test_invalid_key_is_401(client, payload) -> None:
    assert post(client, payload, key="not-a-real-key").status_code == 401


def test_both_keys_of_a_station_are_accepted(client) -> None:
    now = datetime.now(UTC)
    first = make_payload(timestamp=now - timedelta(seconds=10))
    second = make_payload(timestamp=now - timedelta(seconds=20))
    assert post(client, first, key=KEYS["GDL-001"][0]).status_code == 201
    assert post(client, second, key=KEYS["GDL-001"][1]).status_code == 201


def test_key_of_other_station_is_403(client, payload) -> None:
    response = post(client, payload, key=KEYS["GDL-002"][0])
    assert response.status_code == 403
    assert response.headers["content-type"] == PROBLEM


# --- 415 / 413 / 400 -------------------------------------------------------------


@pytest.mark.parametrize(
    "content_type", ["text/plain", "application/xml", "application/json; charset=latin-1"]
)
def test_unsupported_content_type_is_415(client, payload, content_type) -> None:
    response = post(client, payload, headers={"Content-Type": content_type})
    assert response.status_code == 415


def test_json_with_utf8_charset_is_accepted(client, payload) -> None:
    response = post(client, payload, headers={"Content-Type": "application/json; charset=utf-8"})
    assert response.status_code == 201


def _oversized() -> bytes:
    return json.dumps({"padding": "x" * 5000}).encode()


def test_body_over_4kb_is_413(client) -> None:
    assert post(client, _oversized()).status_code == 413


def test_chunked_body_over_4kb_without_content_length_is_413(client) -> None:
    data = _oversized()

    def chunks() -> Iterator[bytes]:
        for i in range(0, len(data), 512):
            yield data[i : i + 512]

    response = client.post(
        URL,
        content=chunks(),
        headers={"Content-Type": "application/json", "X-Station-Key": KEYS["GDL-001"][0]},
    )
    assert response.status_code == 413


def test_body_of_exactly_4096_bytes_is_not_413(client) -> None:
    data = json.dumps(make_payload())
    data = data[:-1] + " " * (4096 - len(data)) + "}"
    assert len(data.encode()) == 4096
    assert post(client, data).status_code == 201


@pytest.mark.parametrize(
    ("raw", "line", "column"),
    [
        ('{"schema_version": "1.0",\n  "station_id": }', 2, 17),
        ("", 1, 1),
        ('{"a": NaN}', 1, 7),
    ],
    ids=["missing-value", "empty-body", "nan"],
)
def test_malformed_json_is_400_with_position(client, raw, line, column) -> None:
    response = post(client, raw)
    assert response.status_code == 400
    assert response.headers["content-type"] == PROBLEM
    body = response.json()
    assert (body["line"], body["column"]) == (line, column)


def test_invalid_utf8_is_400(client) -> None:
    response = post(client, b'{"a": "\xff"}')
    assert response.status_code == 400
    assert (response.json()["line"], response.json()["column"]) == (1, 8)


def test_deeply_nested_json_is_400(client) -> None:
    assert post(client, "[" * 2000 + "]" * 2000).status_code == 400


# --- 422 -------------------------------------------------------------------------


def test_schema_error_is_422_with_field_errors(client, mutate) -> None:
    data = mutate(lambda d: d["measurements"].update(relative_humidity_pct=101))
    response = post(client, data)
    assert response.status_code == 422
    assert response.headers["content-type"] == PROBLEM
    assert response.json()["errors"] == [
        {
            "field": "measurements.relative_humidity_pct",
            "code": "less_than_equal",
            "message": "Input should be less than or equal to 100",
            "value": 101,
        }
    ]


def test_missing_field_error_does_not_echo_payload(client, mutate) -> None:
    data = mutate(lambda d: d.pop("measurements"))
    [error] = post(client, data).json()["errors"]
    assert (error["field"], error["code"], error["value"]) == ("measurements", "missing", None)


def test_non_object_body_is_422(client) -> None:
    [error] = post(client, [1, 2]).json()["errors"]
    assert (error["field"], error["code"]) == ("(body)", "model_type")


def test_timestamp_window_comes_from_settings(settings: Settings, mutate) -> None:
    narrow = Settings(**{**settings.__dict__, "max_past_age": timedelta(seconds=5)})
    data = mutate(lambda d: d.update(timestamp=iso_z(datetime.now(UTC) - timedelta(minutes=1))))
    with TestClient(create_app(narrow)) as client:
        response = post(client, data)
    assert response.status_code == 422
    assert response.json()["errors"][0]["code"] == "timestamp_too_old"


# --- orden de comprobaciones -----------------------------------------------------


def test_order_401_before_415_413_400(client) -> None:
    assert post(client, "not json", key=None, headers={"Content-Type": "text/plain"}).status_code == 401


def test_order_415_before_413(client) -> None:
    response = post(client, _oversized(), headers={"Content-Type": "text/plain"})
    assert response.status_code == 415


def test_order_413_before_400(client) -> None:
    assert post(client, b"{" * 5000).status_code == 413


def test_order_422_before_403(client, mutate) -> None:
    data = mutate(lambda d: d["measurements"].update(relative_humidity_pct=101))
    assert post(client, data, key=KEYS["GDL-002"][0]).status_code == 422


def test_order_403_before_409(client, payload) -> None:
    assert post(client, payload).status_code == 201
    assert post(client, payload, key=KEYS["GDL-002"][0]).status_code == 403


# --- handler de RequestValidationError (rutas con cuerpo declarado) ---------------


def test_request_validation_handler_separates_400_and_422(settings: Settings) -> None:
    app = create_app(settings)

    @app.post("/_probe")
    def probe(reading: WeatherReading) -> dict:
        return {}

    with TestClient(app) as client:
        malformed = client.post(
            "/_probe", content='{\n  "a": }', headers={"Content-Type": "application/json"}
        )
        invalid = client.post("/_probe", json={"schema_version": "2.0"})
    assert malformed.status_code == 400
    assert malformed.headers["content-type"] == PROBLEM
    assert (malformed.json()["line"], malformed.json()["column"]) == (2, 8)
    assert invalid.status_code == 422
    assert {"field": "schema_version", "code": "literal_error"}.items() <= invalid.json()["errors"][0].items()


# --- 500, salud, documentación y logs ---------------------------------------------


class _BrokenStorage(StorageAdapter):
    def save(self, stored: StoredReading) -> None:
        raise OSError("disco lleno: /secret/path")


def test_unexpected_error_is_generic_500(settings: Settings, payload) -> None:
    app = create_app(settings, storage=_BrokenStorage())
    with TestClient(app, raise_server_exceptions=False) as client:
        response = post(client, payload)
    assert response.status_code == 500
    assert response.headers["content-type"] == PROBLEM
    assert response.json()["detail"] == "Error interno"
    assert "secret" not in response.text and "Traceback" not in response.text


def test_health_docs_and_openapi(client) -> None:
    assert client.get("/health").json() == {"status": "ok"}
    assert client.get("/docs").status_code == 200
    spec = client.get("/openapi.json").json()
    operation = spec["paths"][URL]["post"]
    assert operation["requestBody"]["content"]["application/json"]["schema"] == {
        "$ref": "#/components/schemas/WeatherReading"
    }
    assert {"WeatherReading", "Measurements", "Device", "Problem"} <= spec["components"]["schemas"].keys()
    assert set(operation["responses"]) >= {"201", "400", "401", "403", "409", "413", "415", "422"}


def test_logs_have_ids_and_never_the_key_or_payload(client, payload, log_records) -> None:
    key = KEYS["GDL-001"][0]
    post(client, payload, key=key)
    post(client, payload, key=key)
    post(client, payload, key="wrong-key")
    events = [r for r in log_records if getattr(r, "event", None) == "reading"]
    assert [r.status for r in events] == [201, 409, 401]
    assert events[0].reading_id and events[0].station_id == "GDL-001"
    for record in log_records:
        dumped = json.dumps(record.__dict__, default=str)
        assert key not in dumped and "wrong-key" not in dumped
        assert "841.3" not in dumped
```

=== ingest_api/tests/test_storage.py === (98 líneas, sha256: 3ef82457e49b8dfe67761ac43685b9a4a8cc0f13d09be92dbc44171df7c2c1a6)

```python
import json
import threading
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from app.models import WeatherReading
from app.storage import DuplicateReadingError, JsonlStorage, StoredReading
from tests.conftest import make_payload

BASE = datetime.now(UTC).replace(microsecond=0) - timedelta(hours=1)


def stored(offset_s: int = 0, station_id: str = "GDL-001") -> StoredReading:
    data = make_payload(station_id=station_id, timestamp=BASE + timedelta(seconds=offset_s))
    return StoredReading(
        reading_id=f"r-{offset_s}",
        received_at=datetime.now(UTC),
        reading=WeatherReading.model_validate_json(json.dumps(data)),
    )


def run_concurrently(targets) -> None:
    barrier = threading.Barrier(len(targets))

    def wrap(target):
        def run():
            barrier.wait()
            target()

        return run

    threads = [threading.Thread(target=wrap(t)) for t in targets]
    for t in threads:
        t.start()
    for t in threads:
        t.join()


def test_concurrent_distinct_saves_write_whole_lines(tmp_path: Path) -> None:
    storage = JsonlStorage(tmp_path / "r.jsonl")
    run_concurrently([lambda i=i: storage.save(stored(i)) for i in range(50)])
    storage.close()
    raw_lines = (tmp_path / "r.jsonl").read_text(encoding="utf-8").splitlines()
    records = [json.loads(line) for line in raw_lines]
    assert len(records) == 50
    assert len({r["reading_id"] for r in records}) == 50


def test_concurrent_same_reading_saves_once(tmp_path: Path) -> None:
    storage = JsonlStorage(tmp_path / "r.jsonl")
    outcomes: list[str] = []
    lock = threading.Lock()

    def attempt() -> None:
        try:
            storage.save(stored(0))
            result = "created"
        except DuplicateReadingError:
            result = "duplicate"
        with lock:
            outcomes.append(result)

    run_concurrently([attempt] * 20)
    storage.close()
    assert outcomes.count("created") == 1
    assert outcomes.count("duplicate") == 19


def test_index_rebuild_skips_corrupt_lines_and_keeps_appending_cleanly(tmp_path: Path) -> None:
    path = tmp_path / "r.jsonl"
    first = JsonlStorage(path)
    first.save(stored(0))
    first.close()
    with open(path, "ab") as f:
        f.write(b"not json\n")
        f.write(b'{"reading_id": "truncated", "reading": {"station_')  # sin salto de línea

    second = JsonlStorage(path)
    assert second.indexed_count == 1
    with pytest.raises(DuplicateReadingError):
        second.save(stored(0))
    second.save(stored(1))
    second.close()

    last = path.read_text(encoding="utf-8").splitlines()[-1]
    assert json.loads(last)["reading_id"] == "r-1"


def test_record_format(tmp_path: Path) -> None:
    storage = JsonlStorage(tmp_path / "r.jsonl")
    storage.save(stored(0))
    storage.close()
    [record] = [json.loads(line) for line in (tmp_path / "r.jsonl").read_text().splitlines()]
    assert set(record) == {"reading_id", "received_at", "reading"}
    assert record["reading"]["timestamp"].endswith("Z")
    assert record["received_at"].endswith("Z")
```

=== ingest_api/tests/test_models.py === (118 líneas, sha256: eaec711df6c4c377c25987539bbec505ac73bac550a99eeed9023b5755b54b0c)

```python
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from pydantic import ValidationError

from app.models import WeatherReading, weather_reading_json_schema
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
```

=== ingest_api/tests/test_gen_station_key.py === (20 líneas, sha256: 96e911ca30cf55ebb147ab7d4de67e87dfee03ecaee5261372740bd8946991ab)

```python
import base64
import subprocess
import sys
from pathlib import Path

from app.auth import hash_key

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "gen_station_key.py"


def test_generated_key_is_32_bytes_urlsafe_and_hash_matches_api() -> None:
    result = subprocess.run(
        [sys.executable, str(SCRIPT)], capture_output=True, text=True, check=True
    )
    lines = dict(line.split("=", 1) for line in result.stdout.splitlines())
    assert set(lines) == {"STATION_KEY", "SHA256"}
    key = lines["STATION_KEY"]
    assert len(base64.urlsafe_b64decode(key + "=" * (-len(key) % 4))) == 32
    assert lines["SHA256"] == hash_key(key)
    assert result.stdout.count(key) == 1
```

=== ingest_api/tests/__init__.py === (0 líneas, sha256: e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855)

```python
```

=== docs/adr/0001-ingesta-y-persistencia.md === (73 líneas, sha256: e6259904f0e998367c2a6e7a2ee31cce1b9508b3fce419ccc237d00e50d41f97)

```markdown
# ADR 0001 — Ingesta y persistencia de lecturas meteorológicas

- **Estado:** Aceptada
- **Fecha:** 2026-10-01
- **Alcance:** Fase A (contrato y API de ingesta). Ejecución solo en Docker local; Azure es el destino que describe el C4.

## Contexto

Skilliket necesita un prototipo que emule una estación meteorológica en Docker, genere datos sintéticos y los envíe a un endpoint REST que reciba y valide payloads JSON. La infraestructura objetivo es Azure. Hubo que decidir cómo entran los datos (IoT Hub o API propia) y dónde se guardan.

## Decisión 1 — Ingesta: API REST propia (FastAPI + Pydantic v2)

La estación envía `POST /v1/readings` por HTTPS al **Ingest API**. El objetivo en Azure es **Azure Container Apps** (plan Consumption). En esta fase solo corre en local.

### Razones

- El entregable pide un endpoint REST que **valide** el JSON y responda al dispositivo. IoT Hub no valida el cuerpo de los mensajes: solo los enruta. Habría que añadir un consumidor aparte, que además no puede devolver un 4xx a la estación.
- La misma imagen Docker que corre en local se despliega sin cambios en Container Apps. La plataforma da TLS en la ingress, escalado a cero, health probes, secretos con referencias a Key Vault e identidad administrada.
- App Service F1 queda descartado: CPU compartida limitada a 60 min/día, y Microsoft no lo admite para producción.
- Con un solo modelo Pydantic se obtienen la validación, el JSON Schema publicado (`schema/weather_reading.v1.schema.json`) y OpenAPI, así que no pueden desincronizarse.

### Alternativa descartada y ruta de evolución: Azure IoT Hub

IoT Hub aporta una identidad por dispositivo (SAS con clave simétrica o X.509), MQTT/AMQP y enrutamiento a Storage, Event Hubs, Service Bus o Cosmos DB. Su nivel Free da 8,000 mensajes/día con medidor de 0.5 KB y un hub gratuito por suscripción. Se reconsiderará cuando:

- el número de estaciones o la frecuencia superen lo razonable para API keys gestionadas a mano,
- se necesiten comandos o configuración nube→dispositivo (device twins, C2D),
- o se exija autenticación por certificado X.509 con aprovisionamiento (DPS).

En ese escenario, la lógica de validación de este API se reutiliza como consumidor del endpoint compatible con Event Hubs.

## Decisión 2 — Autenticación de estaciones

- Cabecera `X-Station-Key` con una clave aleatoria de 32 bytes (`scripts/gen_station_key.py`).
- El servidor recibe **solo hashes SHA-256** por la variable de entorno `STATION_KEY_HASHES` (`{station_id: [hash1, hash2]}`). Cada estación tiene una o dos claves, para rotar sin cortes. Un hash no puede repetirse entre estaciones.
- La comparación usa `hmac.compare_digest` y recorre todas las entradas.
- Si el `station_id` del cuerpo no corresponde a la clave, la respuesta es 403.
- Las claves en claro no se escriben en archivos versionados ni en logs. Las pruebas generan sus claves al ejecutarse.
- **Objetivo en Azure:** hashes como secreto de Container Apps con referencia a Key Vault, leídos con identidad administrada (rol *Key Vault Secrets User*). Evolución posible: JWT de Entra ID (client credentials) o mTLS en la ingress.

## Decisión 3 — Persistencia

- Interfaz `StorageAdapter.save()`, **síncrona**. Una lectura solo se confirma después de escribirse.
- Implementación local `JsonlStorage`: añade una línea JSON por lectura validada a `STORAGE_PATH` (por defecto `./data/readings.jsonl`, ignorado por git). Cada línea es `{reading_id, received_at, reading}`, donde `reading` es el payload normalizado. La escritura ocurre bajo un lock, como una sola operación `write` de la línea completa, seguida de `flush` y `fsync`.
- **Destino en Azure (no implementado):** un adaptador para Azure Blob Storage (append blobs o un blob por lectura), autenticado con identidad administrada. La interfaz no cambia.

## Cambios respecto a la propuesta inicial y sus razones

1. **Se quita `dew_point_c` del payload.** Es un dato derivado de la temperatura y la HR. Si la estación lo enviara, habría dos fuentes de verdad que podrían contradecirse. Si llega, se rechaza como campo extra (`extra_forbidden`, 422).
2. **Éxito = `201 Created`** (no 202), porque la persistencia es síncrona: cuando se responde, la lectura ya está en disco. Cuerpo: `{status: "created", reading_id, station_id, received_at}`.
3. **Duplicado (`station_id` + `timestamp`) → `409`.** El índice de duplicados es un `set` en memoria que se reconstruye leyendo el archivo al arrancar. La comparación es por instante, no por texto: `21:15:00Z` y `21:15:00.000Z` son la misma lectura. **Los clientes deben tratar 409 como éxito al reintentar**, porque significa que la lectura ya estaba guardada (p. ej. si se perdió la respuesta del primer intento).
4. **`wind_direction_deg = null` con velocidad < 0.5 m/s es comportamiento del generador, no una regla de rechazo.** El API acepta `null` y también un valor numérico con velocidad baja, porque un anemómetro real puede reportar dirección con viento débil.

## Hallazgo de implementación: el cuerpo se lee a mano

El orden de comprobaciones acordado es 401 → 415 → 413 → 400 → 422 → 403 → 409 → 201. Si el cuerpo se declara como parámetro del endpoint, FastAPI (`fastapi/routing.py`, versión 0.142.2) **lee el cuerpo completo sin tope y lo parsea como JSON antes de resolver las dependencias**, autenticación incluida. Con eso, un JSON mal formado sin clave daría 400 en lugar de 401, y el límite de 4 KB solo se aplicaría después de leer todo el cuerpo.

Por eso `POST /v1/readings` hace lo siguiente:

- lee el cuerpo con `request.stream()`, con un tope de 4,096 bytes; también funciona en envíos *chunked* sin `Content-Length`;
- parsea el JSON con `json.loads` para reportar línea y columna;
- valida con `WeatherReading.model_validate_json`.

El handler de `RequestValidationError` también se sobrescribe (JSON inválido → 400, esquema → 422, mismo formato problem+json), pero `/v1/readings` no depende de él. En OpenAPI, el `requestBody` se declara manualmente con una referencia al modelo.

## Límites conocidos

- **Memoria del índice de duplicados:** crece con cada lectura guardada y nunca se poda. Medido con `tracemalloc` en Python 3.13: ~194 bytes por clave. Por ejemplo, 5 estaciones a 1 lectura/min suman ~2.6 M de claves al año, ≈ 510 MB. Mitigación futura: podar las claves fuera de la ventana de aceptación (7 días), porque un timestamp más viejo ya se rechaza con 422; o pasar el índice a un almacén externo.
- **Arranque proporcional al tamaño del archivo:** el índice se reconstruye leyendo todo el JSONL.
- **Un solo proceso:** el lock y el índice son locales. Con varios workers de uvicorn o varias réplicas, dos procesos podrían aceptar la misma lectura y escribir en el mismo archivo. En Container Apps, este adaptador exige `maxReplicas = 1`. El adaptador de Blob deberá resolver la deduplicación de otra forma (p. ej. un nombre de blob determinista por `station_id` + `timestamp` con escritura condicional).
- **Líneas corruptas:** se ignoran al reconstruir el índice y se registra un aviso con el número de línea. Si la última línea quedó truncada, se cierra con un salto antes de seguir escribiendo. Si una escritura falla a medias (p. ej. disco lleno), la respuesta es 500 y la lectura no entra al índice.
- **Validación estricta de tipos:** `"24.7"` (string), `true` en un campo numérico y `60.0` en un campo entero (`interval_s`, `sequence`, `wind_direction_deg`) se rechazan con 422. El generador debe enviar enteros como enteros.
- **Ventana del timestamp:** depende del reloj del servidor. Se configura con `MAX_FUTURE_SKEW_S` y `MAX_PAST_AGE_S`.
```

=== README.md === (64 líneas, sha256: 93a6c6280b747d803d0b74a2cfeca8de18d5a140a6033d8b204a7c89db31fe68)

````markdown
# Skilliket — Estación meteorológica simulada

Prototipo del TEC de Monterrey GDA. Una estación meteorológica emulada envía lecturas a un API REST que las valida y las guarda. Esta entrega corre solo en local; el objetivo en Azure se describe en `docs/adr/` y en el diagrama C4 (fase posterior).

## Estado

- **Fase A (este repo):** contrato v1 (`schema/`) y API de ingesta (`ingest_api/`).
- **Pendiente:** generador de la estación, Docker/compose y diagramas C4.

## Estructura

| Ruta | Contenido |
|---|---|
| `ingest_api/app/` | API FastAPI: modelo, autenticación, errores, almacenamiento |
| `ingest_api/tests/` | Pruebas (pytest) |
| `schema/weather_reading.v1.schema.json` | JSON Schema exportado del modelo Pydantic |
| `scripts/gen_station_key.py` | Genera una clave de estación y su hash SHA-256 |
| `scripts/export_schema.py` | Regenera el JSON Schema |
| `docs/adr/` | Decisiones de arquitectura |

## Ejecutar en local

Requiere Python 3.13.

```powershell
py -3.13 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r ingest_api\requirements-dev.txt

# Una clave por línea de salida: guarda STATION_KEY para la estación y usa SHA256 en el API.
.\.venv\Scripts\python.exe scripts\gen_station_key.py

$env:STATION_KEY_HASHES = '{"GDL-001": ["<sha256>"]}'
cd ingest_api
..\.venv\Scripts\python.exe -m uvicorn app.main:create_app --factory --port 8000
```

- Documentación interactiva: http://127.0.0.1:8000/docs
- Variables de entorno: ver `.env.example`.

## Pruebas

```powershell
cd ingest_api
..\.venv\Scripts\python.exe -m pytest
```

Si cambias `app/models.py`, regenera el esquema con `python scripts/export_schema.py`. Una prueba falla si el archivo exportado no coincide con el modelo.

## API

`POST /v1/readings` con `Content-Type: application/json` y la cabecera `X-Station-Key`.

| Código | Caso |
|---|---|
| 201 | Lectura válida guardada |
| 400 | JSON mal formado (`line`, `column`) |
| 401 | Falta la clave o no es válida |
| 403 | El `station_id` del cuerpo no corresponde a la clave |
| 409 | Duplicado (`station_id` + `timestamp`): al reintentar, tratar como éxito |
| 413 | Cuerpo mayor a 4 KB |
| 415 | Content-Type distinto de `application/json` |
| 422 | No cumple el esquema (`errors: [{field, code, message, value}]`) |

Los errores usan `application/problem+json` (RFC 9457). `GET /health` no requiere autenticación.
````

=== .gitignore === (12 líneas, sha256: 449ecefbeb499861b84b186ad783c8b9d57d5d2ccfc490e6c0e85e726a0e628c)

```gitignore
# Entornos y caches de Python
.venv/
__pycache__/
*.py[cod]
.pytest_cache/
.pytest_tmp/

# Datos locales de ingesta (JSON Lines) y logs de pruebas
data/

# Variables de entorno reales (pueden contener hashes de claves)
.env
```

=== .env.example === (12 líneas, sha256: c2c2cc2e49a9a5b77dbefe0fc6278c937669ad0d8ae3609268d4047df9626bc6)

```dotenv
# Copia este archivo a .env y reemplaza los valores. Nunca pongas claves en claro aquí:
# solo hashes SHA-256 (hex, minúsculas) generados con scripts/gen_station_key.py.

# JSON {station_id: [hash_clave_1, hash_clave_2]}. Una o dos claves por estación (rotación).
STATION_KEY_HASHES={"GDL-001": ["<sha256-hex-clave-primaria>", "<sha256-hex-clave-secundaria>"]}

# Archivo JSON Lines donde se guarda cada lectura validada.
STORAGE_PATH=./data/readings.jsonl

# Ventana de aceptación del timestamp, en segundos.
MAX_FUTURE_SKEW_S=300
MAX_PAST_AGE_S=604800
```

=== ingest_api/requirements.txt === (17 líneas, sha256: 6bab3c7a307200e7baf27d2f872a7c1461ebffa3f129f5e5b0a68b65aa9bab37)

```text
# Dependencias de ejecución, versiones exactas (incluye transitivas).
# Probadas con Python 3.13.5; la imagen objetivo es python:3.13-slim.
fastapi==0.142.2
pydantic==2.13.5
uvicorn==0.54.0
annotated-doc==0.0.5
annotated-types==0.8.0
anyio==4.15.1
click==8.5.0
colorama==0.4.6; sys_platform == "win32"
h11==0.16.0
idna==3.20
opentelemetry-api==1.45.0
pydantic_core==2.46.5
starlette==1.7.0
typing-inspection==0.4.4
typing_extensions==4.16.0
```

=== ingest_api/requirements-dev.txt === (12 líneas, sha256: ca2cb65fd1953560858dd807b100591bc40e28751f879abe26766222a9222baf)

```text
# Dependencias de pruebas, versiones exactas (incluye transitivas).
# httpx2 es el cliente que usa starlette.testclient desde Starlette 1.x (httpx está deprecado ahí).
-r requirements.txt
pytest==9.1.1
httpx2==2.13.1
certifi==2026.7.22
httpcore2==2.13.1
iniconfig==2.3.0
packaging==26.3
pluggy==1.6.0
Pygments==2.21.0
truststore==0.10.4
```

=== ingest_api/pytest.ini === (5 líneas, sha256: 8bbad0254015471de2585c2fad181f7c30b88f2076e238c2425850e45c56e25b)

```ini
[pytest]
pythonpath = .
testpaths = tests
# Temporales de pruebas dentro del repo (ignorados por git); pytest los vacía al iniciar.
addopts = --basetemp=.pytest_tmp
```

