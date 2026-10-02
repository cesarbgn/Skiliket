import json
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta

import pytest
from fastapi.testclient import TestClient

from app.config import Settings
from app.main import create_app
from app.models import WeatherReading
from app.storage import JsonlStorage, StorageAdapter, StoredReading
from tests.conftest import KEYS, FakeClock, iso_z, make_payload

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


def test_key_inside_window_survives_pruning(settings: Settings, payload, monkeypatch) -> None:
    clock = FakeClock(datetime.now(UTC))
    storage = JsonlStorage(settings.storage_path, settings.max_past_age, clock=clock)
    prunes: list[datetime] = []
    original = storage._prune_locked
    monkeypatch.setattr(storage, "_prune_locked", lambda now: (prunes.append(now), original(now)))
    with TestClient(create_app(settings, storage=storage)) as client:
        assert post(client, payload).status_code == 201
        clock.advance(minutes=6)
        other = make_payload(timestamp=datetime.now(UTC) - timedelta(seconds=90))
        assert post(client, other).status_code == 201  # esta escritura dispara la poda
        assert len(prunes) == 1
        assert post(client, payload).status_code == 409


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


def test_body_of_4097_bytes_is_413(client) -> None:
    data = json.dumps(make_payload())
    data = data[:-1] + " " * (4097 - len(data)) + "}"
    assert len(data.encode()) == 4097
    assert post(client, data).status_code == 413


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


# --- 404 / 405 -------------------------------------------------------------------


def test_unknown_route_is_404_problem(client) -> None:
    response = client.post("/nope")
    assert response.status_code == 404
    assert response.headers["content-type"] == PROBLEM
    assert response.json() == {
        "type": "urn:skilliket:problem:not-found",
        "title": "Not Found",
        "status": 404,
        "detail": "Not Found",
        "instance": "/nope",
    }


def test_wrong_method_is_405_problem_with_allow(client) -> None:
    response = client.get(URL)
    assert response.status_code == 405
    assert response.headers["content-type"] == PROBLEM
    assert response.headers["allow"] == "POST"
    body = response.json()
    assert (body["type"], body["title"], body["status"], body["instance"]) == (
        "urn:skilliket:problem:method-not-allowed",
        "Method Not Allowed",
        405,
        URL,
    )


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
