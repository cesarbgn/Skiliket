import json
import logging
import secrets
import threading
from collections.abc import Callable, Iterator
from dataclasses import replace
from datetime import UTC, datetime
from typing import Any

import httpx
import pytest

from station.config import Config, Secret
from station.log import JsonFormatter

# Clave generada en cada ejecución: ninguna clave queda escrita en archivos.
KEY = secrets.token_urlsafe(32)
URL = "http://ingest.test/v1/readings"
# 2026-10-01 00:00:37 hora local (UTC-6): no es múltiplo del intervalo, a propósito.
START_WALL = datetime(2026, 10, 1, 6, 0, 37, tzinfo=UTC).timestamp()


def make_config(**overrides: Any) -> Config:
    base = Config(station_id="GDL-001", station_key=Secret(KEY), ingest_url=URL, seed=7)
    return replace(base, **overrides)


class FakeClock:
    """Reloj y espera inyectables: wait() avanza el tiempo en lugar de dormir.

    `at` registra acciones que se ejecutan cuando el reloj monotónico alcanza un instante
    (p. ej. tumbar el API o pedir la detención)."""

    def __init__(self, stop: threading.Event | None = None, wall: float = START_WALL) -> None:
        self.mono = 1000.0
        self.wall = wall
        self.stop = stop or threading.Event()
        self._actions: list[tuple[float, Callable[[], None]]] = []

    def monotonic(self) -> float:
        return self.mono

    def time(self) -> float:
        return self.wall

    def at(self, offset_s: float, action: Callable[[], None]) -> None:
        self._actions.append((1000.0 + offset_s, action))
        self._actions.sort(key=lambda item: item[0])

    def advance(self, seconds: float) -> None:
        target = self.mono + max(0.0, seconds)
        while self._actions and self._actions[0][0] <= target:
            when, action = self._actions.pop(0)
            delta = max(0.0, when - self.mono)
            self.mono += delta
            self.wall += delta
            action()
            if self.stop.is_set():
                return
        delta = target - self.mono
        self.mono += delta
        self.wall += delta

    def wait(self, seconds: float) -> None:
        if not self.stop.is_set():
            self.advance(seconds)

    def sleep(self, seconds: float) -> None:
        self.advance(seconds)


class FakeApi:
    """API falso sobre httpx.MockTransport: 201 la primera vez, 409 si se repite
    (station_id, timestamp), y fallas programables."""

    def __init__(self, clock: FakeClock | None = None) -> None:
        self.clock = clock
        self.down = False  # True: error de conexión
        self.scripted: list[int | type[Exception]] = []  # respuestas forzadas, en orden
        self.request_delay_s = 0.0  # tiempo que "tarda" cada solicitud (avanza el reloj)
        self.stored: list[dict[str, Any]] = []  # cuerpos aceptados con 201, en orden
        self.requests: list[dict[str, Any]] = []  # todos los cuerpos recibidos, en orden
        self.headers: list[httpx.Headers] = []
        self._keys: set[tuple[str, str]] = set()

    def handler(self, request: httpx.Request) -> httpx.Response:
        if self.clock is not None and self.request_delay_s:
            self.clock.advance(self.request_delay_s)
        if self.down:
            raise httpx.ConnectError("connection refused", request=request)
        body = json.loads(request.content)
        self.requests.append(body)
        self.headers.append(request.headers)
        if self.scripted:
            scripted = self.scripted.pop(0)
            if isinstance(scripted, type):
                raise scripted("scripted", request=request)
            return problem(scripted)
        key = (body.get("station_id"), body.get("timestamp"))
        if key in self._keys:
            return problem(409)
        self._keys.add(key)
        self.stored.append(body)
        return httpx.Response(201, json={"status": "created", "reading_id": "x"})

    def client(self) -> httpx.Client:
        return httpx.Client(transport=httpx.MockTransport(self.handler))


def problem(status: int, errors: list[dict[str, Any]] | None = None) -> httpx.Response:
    body: dict[str, Any] = {"type": f"urn:skilliket:problem:{status}", "title": "t", "status": status}
    if errors is not None:
        body["errors"] = errors
    return httpx.Response(status, json=body, headers={"content-type": "application/problem+json"})


class _ListHandler(logging.Handler):
    def __init__(self) -> None:
        super().__init__()
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)


class LogCapture:
    def __init__(self, handler: _ListHandler) -> None:
        self._handler = handler

    @property
    def records(self) -> list[logging.LogRecord]:
        return self._handler.records

    def lines(self) -> list[str]:
        formatter = JsonFormatter()
        return [formatter.format(r) for r in self._handler.records]

    def events(self, msg: str) -> list[logging.LogRecord]:
        return [r for r in self._handler.records if r.getMessage() == msg]


@pytest.fixture
def logs() -> Iterator[LogCapture]:
    # El logger "skilliket" no propaga a root, así que caplog no lo ve.
    handler = _ListHandler()
    logger = logging.getLogger("skilliket")
    logger.addHandler(handler)
    previous = logger.level
    logger.setLevel(logging.INFO)
    yield LogCapture(handler)
    logger.removeHandler(handler)
    logger.setLevel(previous)


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
def api(clock: FakeClock) -> FakeApi:
    return FakeApi(clock)
