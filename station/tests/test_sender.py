import json
import logging
import random
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
import pytest

from conftest import KEY, URL, FakeApi, FakeClock, LogCapture, problem
from station.config import Secret
from station.faults import BY_NAME
from station.sender import BACKOFF_MAX_S, Outbox, Pending, Sender, Stats, classify
from station.weather_model import ReadingGenerator, Site

T0 = datetime(2026, 10, 1, 18, 0, tzinfo=UTC)


def payloads(n: int, station_id: str = "GDL-001") -> list[dict[str, Any]]:
    generator = ReadingGenerator(station_id, 60, 1, Site())
    return [generator.next(T0 + timedelta(minutes=i)) for i in range(n)]


def make_outbox(
    api: FakeApi, clock: FakeClock, max_size: int = 1000, client: httpx.Client | None = None
) -> tuple[Outbox, Stats]:
    stats = Stats()
    sender = Sender(client or api.client(), URL, Secret(KEY))
    return Outbox(sender, "GDL-001", max_size, stats, clock.monotonic, random.Random(0)), stats


def drain_all(outbox: Outbox, clock: FakeClock, limit: int = 1000) -> None:
    """Envía hasta vaciar, respetando el backoff con el reloj falso."""
    for _ in range(limit):
        if not len(outbox):
            return
        if clock.monotonic() < outbox.retry_at:
            clock.advance(outbox.retry_at - clock.monotonic())
        outbox.send_next()
    raise AssertionError("el búfer no se vació")


@pytest.mark.parametrize(
    ("status", "outcome"),
    [
        (201, "created"),
        (200, "created"),
        (409, "duplicate"),
        (500, "retry"),
        (502, "retry"),
        (503, "retry"),
        (408, "retry"),
        (429, "retry"),
        (400, "rejected"),
        (401, "rejected"),
        (403, "rejected"),
        (404, "rejected"),
        (413, "rejected"),
        (415, "rejected"),
        (422, "rejected"),
    ],
)
def test_classify(status: int, outcome: str) -> None:
    assert classify(status) == outcome


def test_201_is_created(api: FakeApi, clock: FakeClock, logs: LogCapture) -> None:
    outbox, stats = make_outbox(api, clock)
    outbox.put(Pending(payloads(1)[0]))
    attempt = outbox.send_next()
    assert attempt is not None and attempt.outcome == "created" and attempt.status == 201
    assert len(outbox) == 0 and stats.created == 1
    [record] = logs.events("reading_delivered")
    assert record.levelno == logging.INFO
    assert (record.station_id, record.sequence, record.status, record.attempt) == ("GDL-001", 0, 201, 1)


def test_request_format(api: FakeApi, clock: FakeClock) -> None:
    outbox, _ = make_outbox(api, clock)
    outbox.put(Pending(payloads(1)[0]))
    outbox.send_next()
    headers = api.headers[0]
    assert headers["content-type"] == "application/json"
    assert headers["x-station-key"] == KEY
    body = api.requests[0]
    # Enteros como enteros (json.dumps), nunca 60.0.
    assert type(body["interval_s"]) is int and type(body["sequence"]) is int


def test_409_counts_as_delivered_duplicate(api: FakeApi, clock: FakeClock) -> None:
    outbox, stats = make_outbox(api, clock)
    reading = payloads(1)[0]
    outbox.put(Pending(reading))
    outbox.put(Pending(dict(reading)))
    drain_all(outbox, clock)
    assert (stats.created, stats.duplicates, stats.retries, stats.dropped_by_status) == (1, 1, 0, {})
    assert len(outbox) == 0


def test_503_then_201_keeps_order(api: FakeApi, clock: FakeClock, logs: LogCapture) -> None:
    outbox, stats = make_outbox(api, clock)
    for p in payloads(3):
        outbox.put(Pending(p))
    api.scripted = [503]
    attempt = outbox.send_next()
    assert attempt is not None and attempt.outcome == "retry"
    assert len(outbox) == 3 and outbox.retry_at > clock.monotonic()
    drain_all(outbox, clock)
    assert [b["sequence"] for b in api.stored] == [0, 1, 2]
    assert stats.created == 3 and stats.retries == 1
    [retry] = logs.events("send_retry")
    assert (retry.status, retry.sequence, retry.attempt) == (503, 0, 1)
    first = logs.events("reading_delivered")[0]
    assert (first.sequence, first.attempt) == (0, 2)


@pytest.mark.parametrize("exc", [httpx.ReadTimeout, httpx.ConnectTimeout, httpx.RemoteProtocolError])
def test_timeouts_and_transport_errors_are_retried(
    api: FakeApi, clock: FakeClock, exc: type[Exception], logs: LogCapture
) -> None:
    outbox, stats = make_outbox(api, clock)
    outbox.put(Pending(payloads(1)[0]))
    api.scripted = [exc]
    attempt = outbox.send_next()
    assert attempt is not None and attempt.outcome == "retry" and attempt.error_type == exc.__name__
    drain_all(outbox, clock)
    assert stats.created == 1 and stats.retries == 1
    assert logs.events("send_retry")[0].error_type == exc.__name__


def test_connection_down_with_5_buffered_flushes_in_order(api: FakeApi, clock: FakeClock) -> None:
    outbox, stats = make_outbox(api, clock)
    readings = payloads(5)
    api.down = True
    for p in readings:
        outbox.put(Pending(p))
        clock.advance(10)
        if clock.monotonic() >= outbox.retry_at:
            outbox.send_next()  # falla: se queda en el búfer
    assert len(outbox) == 5 and stats.retries >= 1 and api.stored == []
    api.down = False
    drain_all(outbox, clock)
    assert [b["sequence"] for b in api.stored] == [0, 1, 2, 3, 4]
    assert len({(b["station_id"], b["timestamp"]) for b in api.stored}) == 5
    assert stats.created == 5 and stats.duplicates == 0


def test_backoff_is_exponential_with_jitter_and_capped(api: FakeApi, clock: FakeClock) -> None:
    outbox, _ = make_outbox(api, clock)
    outbox.put(Pending(payloads(1)[0]))
    api.down = True
    delays = []
    for _ in range(12):
        before = clock.monotonic()
        outbox.send_next()
        delays.append(outbox.retry_at - before)
        clock.advance(outbox.retry_at - before)
    for n, delay in enumerate(delays, start=1):
        base = min(BACKOFF_MAX_S, 2 ** (n - 1))
        assert base * 0.5 <= delay <= base
    assert max(delays) <= BACKOFF_MAX_S


def test_overflow_drops_oldest(api: FakeApi, clock: FakeClock, logs: LogCapture) -> None:
    outbox, stats = make_outbox(api, clock, max_size=3)
    for p in payloads(5):
        outbox.put(Pending(p))
    assert outbox.sequences() == [2, 3, 4]
    assert stats.dropped_overflow == 2
    warnings = logs.events("buffer_overflow")
    assert [w.dropped_sequence for w in warnings] == [0, 1]
    assert all(w.levelno == logging.WARNING for w in warnings)
    drain_all(outbox, clock)
    assert [b["sequence"] for b in api.stored] == [2, 3, 4]


@pytest.mark.parametrize("status", [400, 401, 403, 413, 415, 422])
def test_non_retryable_are_dropped_without_retry(
    api: FakeApi, clock: FakeClock, logs: LogCapture, status: int
) -> None:
    outbox, stats = make_outbox(api, clock)
    outbox.put(Pending(payloads(1)[0]))
    api.scripted = [status]
    attempt = outbox.send_next()
    assert attempt is not None and attempt.outcome == "rejected"
    assert len(outbox) == 0 and len(api.requests) == 1
    assert stats.retries == 0 and stats.dropped_by_status == {str(status): 1}
    assert outbox.retry_at == 0.0
    [record] = [r for r in logs.records if r.getMessage().startswith("reading_rejected")]
    assert record.status == status
    if status in (401, 403):
        assert record.levelno == logging.ERROR
        assert record.reason == ("check_station_key" if status == 401 else "check_station_id")
        assert ("STATION_KEY" if status == 401 else "STATION_ID") in record.getMessage()
    else:
        assert record.levelno == logging.WARNING


def test_401_does_not_loop(api: FakeApi, clock: FakeClock) -> None:
    outbox, stats = make_outbox(api, clock)
    for p in payloads(3):
        outbox.put(Pending(p))
    api.scripted = [401, 401, 401]
    drain_all(outbox, clock)
    # Una solicitud por lectura, ninguna repetida.
    assert [b["sequence"] for b in api.requests] == [0, 1, 2]
    assert stats.dropped_by_status == {"401": 3} and stats.retries == 0


def test_422_logs_field_code_summary(api: FakeApi, clock: FakeClock, logs: LogCapture) -> None:
    error = {"field": "measurements.relative_humidity_pct", "code": "less_than_equal", "value": 101}

    def respond(request: httpx.Request) -> httpx.Response:
        return problem(422, [error])

    client = httpx.Client(transport=httpx.MockTransport(respond))
    outbox, _ = make_outbox(api, clock, client=client)
    outbox.put(Pending(payloads(1)[0]))
    outbox.send_next()
    [record] = logs.events("reading_rejected")
    assert record.errors == [["measurements.relative_humidity_pct", "less_than_equal"]]
    assert record.problem_type == "urn:skilliket:problem:422"
    # El valor devuelto por el API no se registra.
    assert "101" not in json.dumps(record.errors)


def _reply_422(field: str, code: str) -> httpx.Client:
    def respond(request: httpx.Request) -> httpx.Response:
        return problem(422, [{"field": field, "code": code, "message": "m", "value": None}])

    return httpx.Client(transport=httpx.MockTransport(respond))


def test_fault_confirmed_and_mismatch(api: FakeApi, clock: FakeClock, logs: LogCapture) -> None:
    fault = BY_NAME["humidity_over_100"]
    outbox, stats = make_outbox(api, clock, client=_reply_422(fault.field, fault.code))
    outbox.put(Pending(payloads(1)[0], fault))
    outbox.send_next()
    assert stats.faults_confirmed == 1 and stats.faults_unexpected == 0
    [ok] = logs.events("fault_confirmed")
    assert ok.fault == "humidity_over_100" and ok.received["status"] == 422

    outbox, stats = make_outbox(api, clock, client=_reply_422(fault.field, "other_code"))
    outbox.put(Pending(payloads(1)[0], fault))
    outbox.send_next()
    assert stats.faults_unexpected == 1 and stats.faults_confirmed == 0
    [bad] = logs.events("fault_mismatch")
    assert bad.levelno == logging.WARNING
    assert bad.expected == {"status": 422, "errors": [[fault.field, fault.code]]}
    assert bad.received == {"status": 422, "errors": [[fault.field, "other_code"]]}


def test_fault_accepted_with_201_is_mismatch(api: FakeApi, clock: FakeClock) -> None:
    outbox, stats = make_outbox(api, clock)
    outbox.put(Pending(payloads(1)[0], BY_NAME["extra_field"]))
    outbox.send_next()
    assert stats.created == 1 and stats.faults_unexpected == 1 and stats.faults_confirmed == 0


def test_logs_never_contain_key_or_full_payload(api: FakeApi, clock: FakeClock, logs: LogCapture) -> None:
    # 8 lecturas en un búfer de 6: dos desbordan y las seis restantes recorren 503, timeout,
    # 401, 403, 422 y 400.
    outbox, _ = make_outbox(api, clock, max_size=6)
    readings = payloads(8)
    api.scripted = [503, httpx.ReadTimeout, 401, 403, 422, 400]
    for p in readings:
        outbox.put(Pending(p, BY_NAME["extra_field"]))
    drain_all(outbox, clock)
    text = "\n".join(logs.lines())
    assert logs.records, "se esperaban logs"
    assert KEY not in text
    for p in readings:
        assert json.dumps(p) not in text
        assert json.dumps(p, separators=(",", ":")) not in text
    assert '"measurements"' not in text and "air_temperature_c" not in text
