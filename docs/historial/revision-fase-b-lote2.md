# Revisión Fase B — lote 2 (lote de cierre)

Volcado literal generado por script desde los bytes de cada archivo. Cada bloque está delimitado por una cerca de backticks más larga que cualquier secuencia de backticks del archivo. N = líneas según `wc -l` (saltos de línea); sha256 = hash de los bytes del archivo.

Solo los archivos cuyo sha256 cambió respecto de `revision-fase-b-codigo.md` y `revision-fase-b-pruebas-docs.md`: reanclaje del calendario vivo tras un salto del reloj de pared, sus 4 pruebas y la documentación.

=== station/station/log.py === (57 líneas, sha256: a8821b899becf4857a901d2cec17c81923939d48a82cd4bfc4bd8f8e77ee513b)

```python
"""Logs estructurados (una línea JSON por evento). Nunca incluyen la clave ni el payload completo."""

import json
import logging
import sys
from datetime import UTC, datetime

_EXTRA_FIELDS = (
    "event",
    "station_id",
    "sequence",
    "timestamp",
    "status",
    "attempt",
    "outcome",
    "reason",
    "error_type",
    "problem_type",
    "errors",
    "fault",
    "expected",
    "received",
    "delay_s",
    "buffered",
    "dropped_sequence",
    "summary",
    "config",
    "jump_s",
    "ticks_skipped",
)


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": datetime.fromtimestamp(record.created, UTC).isoformat().replace("+00:00", "Z"),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        for name in _EXTRA_FIELDS:
            if hasattr(record, name):
                payload[name] = getattr(record, name)
        # ASCII escapado: la consola de Windows puede ser cp1252.
        return json.dumps(payload)


def configure_logging() -> None:
    logger = logging.getLogger("skilliket")
    if any(getattr(h, "_skilliket", False) for h in logger.handlers):
        return
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    handler._skilliket = True  # type: ignore[attr-defined]
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False
```

=== station/station/main.py === (289 líneas, sha256: a25b14304ac4b7ca2f761689cd1a81c14777108ddffdd11e08856f241b930435)

```python
"""Bucle principal de la estación. Arranque: python -m station.main

Modo vivo: un tick por intervalo, programado sobre el reloj monotónico a partir del
arranque (sin deriva acumulada); el timestamp de cada tick es un múltiplo del intervalo
(piso del reloj de pared) y nunca queda en el futuro. Modo réplica (REPLAY_HOURS > 0):
primero las lecturas de las últimas N horas repartidas en REPLAY_DURATION_MIN, luego el
modo vivo con el mismo modelo y la misma secuencia. La réplica tiene contrapresión: su
lectura siguiente se genera cuando el búfer está vacío, así que nunca lo desborda.

Salto del reloj de pared en modo vivo (p. ej. equipo suspendido: la pared avanza y el
monotónico no): si la pared va más de max(2 x intervalo, 5 s) por delante del calendario,
se reancla al piso del reloj de pared y se omiten los ticks perdidos (una estación
suspendida no mide). Los saltos hacia atrás no se corrigen: se espera a que la pared
alcance el siguiente timestamp, para no repetir timestamps ya guardados.

Fin: con MAX_READINGS, al entregar o descartar todas las lecturas generadas; con
SIGINT/SIGTERM/SIGBREAK, tras intentar vaciar el búfer durante 10 s como máximo."""

import logging
import os
import signal
import sys
import threading
import time
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any, Protocol

import httpx

from station.config import Config, ConfigError, load_config
from station.faults import FaultInjector
from station.log import configure_logging
from station.sender import Outbox, Pending, Sender, Stats
from station.weather_model import ReadingGenerator, Site

logger = logging.getLogger("skilliket.station")

DRAIN_TIMEOUT_S = 10.0
_WAIT_SLICE_S = 0.5  # la espera se parte para atender señales pronto (también en Windows)
_WALL_SKEW_WAIT_S = 0.05
_MIN_JUMP_S = 5.0


class Clock(Protocol):
    def monotonic(self) -> float: ...

    def time(self) -> float: ...

    def wait(self, seconds: float) -> None:
        """Espera hasta `seconds`; vuelve antes si se pidió detener la estación."""

    def sleep(self, seconds: float) -> None:
        """Espera `seconds` aunque se haya pedido detener (para el vaciado final)."""


class SystemClock:
    def __init__(self, stop: threading.Event) -> None:
        self._stop = stop

    def monotonic(self) -> float:
        return time.monotonic()

    def time(self) -> float:
        return time.time()

    def wait(self, seconds: float) -> None:
        end = time.monotonic() + seconds
        while not self._stop.is_set():
            remaining = end - time.monotonic()
            if remaining <= 0:
                return
            self._stop.wait(min(remaining, _WAIT_SLICE_S))

    def sleep(self, seconds: float) -> None:
        end = time.monotonic() + seconds
        while (remaining := end - time.monotonic()) > 0:
            time.sleep(min(remaining, _WAIT_SLICE_S))


class Schedule:
    """Secuencia de (instante monotónico en que toca, timestamp en segundos epoch)."""

    def __init__(self, cfg: Config, start_wall: float, start_mono: float) -> None:
        self._interval = cfg.interval_s
        self._start_mono = start_mono
        self.floor_start = int(start_wall // cfg.interval_s) * cfg.interval_s
        self.replay_count = cfg.replay_hours * 3600 // cfg.interval_s
        self._spacing = (
            cfg.replay_duration_min * 60 / self.replay_count if self.replay_count else 0.0
        )
        self.index = 0
        # Ancla del modo vivo: (pared, monotónico) de referencia y el primer tick vivo.
        # Sin réplica, la primera lectura viva es el piso del arranque (se emite de inmediato);
        # con réplica, ese instante ya lo cubrió la última lectura de la réplica.
        self._anchor_wall = start_wall
        self._anchor_mono = start_mono
        self._live_first_ts = self.floor_start + (cfg.interval_s if self.replay_count else 0)
        self._live_first_index = self.replay_count
        self._jump_threshold_s = max(2.0 * cfg.interval_s, _MIN_JUMP_S)

    def peek(self) -> tuple[float, int]:
        k = self.index
        if k < self.replay_count:
            ts = self.floor_start - (self.replay_count - 1 - k) * self._interval
            return self._start_mono + k * self._spacing, ts
        ts = self._live_first_ts + (k - self._live_first_index) * self._interval
        return self._anchor_mono + (ts - self._anchor_wall), ts

    def reanchor_if_wall_jumped(self, mono: float, wall: float) -> tuple[float, int] | None:
        """En modo vivo, si la pared saltó hacia adelante, reancla y devuelve
        (salto en s, ticks omitidos); si no, None. Los saltos hacia atrás no se tocan."""
        if self.in_replay:
            return None
        drift = wall - (self._anchor_wall + (mono - self._anchor_mono))
        if drift <= self._jump_threshold_s:
            return None
        _, next_ts = self.peek()
        last_ts = next_ts - self._interval  # último generado (o el piso, si aún no hay)
        new_first = max(int(wall // self._interval) * self._interval, last_ts + self._interval)
        self._anchor_wall, self._anchor_mono = wall, mono
        self._live_first_ts, self._live_first_index = new_first, self.index
        return drift, (new_first - next_ts) // self._interval

    @property
    def in_replay(self) -> bool:
        return self.index < self.replay_count

    def advance(self) -> None:
        self.index += 1


def _drain(outbox: Outbox, clock: Clock, read_timeout: float) -> None:
    """Al detenerse por señal, intenta vaciar el búfer durante DRAIN_TIMEOUT_S como máximo."""
    deadline = clock.monotonic() + DRAIN_TIMEOUT_S
    outbox.retry_now()
    while len(outbox):
        now = clock.monotonic()
        if now >= deadline:
            return
        if now < outbox.retry_at:
            clock.sleep(min(outbox.retry_at, deadline) - now)
            continue
        outbox.send_next(timeout=min(read_timeout, deadline - now))


def run(cfg: Config, client: httpx.Client, clock: Clock, stop: threading.Event) -> tuple[Stats, str]:
    stats = Stats()
    site = Site(cfg.lat, cfg.lon, cfg.elevation_m, cfg.tz_offset_h, cfg.prevailing_wind_deg)
    generator = ReadingGenerator(cfg.station_id, cfg.interval_s, cfg.seed, site)
    injector = FaultInjector(cfg.fault_rate, cfg.seed)
    sender = Sender(client, cfg.ingest_url, cfg.station_key)
    outbox = Outbox(sender, cfg.station_id, cfg.buffer_max, stats, clock.monotonic)
    schedule = Schedule(cfg, clock.time(), clock.monotonic())
    logger.info(
        "station_started",
        extra={
            "event": "station_started",
            "station_id": cfg.station_id,
            "config": {
                "ingest_url": cfg.ingest_url,
                "interval_s": cfg.interval_s,
                "seed": cfg.seed,
                "replay_readings": schedule.replay_count,
                "replay_duration_min": cfg.replay_duration_min,
                "fault_rate": cfg.fault_rate,
                "buffer_max": cfg.buffer_max,
                "max_readings": cfg.max_readings,
            },
        },
    )

    exit_reason = "signal"
    while not stop.is_set():
        done_generating = bool(cfg.max_readings) and stats.generated >= cfg.max_readings
        if done_generating and not len(outbox):
            # Con MAX_READINGS se termina al entregar (o descartar) todo, no al generar la última.
            exit_reason = "max_readings"
            break
        now = clock.monotonic()
        wall = clock.time()
        waits: list[float] = []
        if not done_generating:
            jump = schedule.reanchor_if_wall_jumped(now, wall)
            if jump is not None:
                logger.warning(
                    "clock_jump",
                    extra={
                        "event": "clock_jump",
                        "station_id": cfg.station_id,
                        "jump_s": round(jump[0], 1),
                        "ticks_skipped": jump[1],
                    },
                )
            due, ts_epoch = schedule.peek()
            # Contrapresión solo en la réplica: su lectura siguiente espera a que el búfer se
            # vacíe; si no, una réplica rápida llenaría el búfer y desbordaría las más viejas.
            # Las lecturas en vivo no esperan: dependen del reloj.
            replay_blocked = schedule.in_replay and len(outbox) > 0
            if not replay_blocked:
                if now >= due and wall >= ts_epoch:
                    ts = datetime.fromtimestamp(ts_epoch, UTC)
                    payload = generator.next(ts)
                    stats.generated += 1
                    now_utc = datetime.fromtimestamp(wall, UTC)
                    payload, fault = injector.maybe_inject(payload, now_utc)
                    if fault is not None:
                        stats.faults_injected += 1
                        logger.info(
                            "fault_injected",
                            extra={
                                "event": "fault_injected",
                                "station_id": cfg.station_id,
                                "sequence": payload["sequence"],
                                "fault": fault.name,
                            },
                        )
                    outbox.put(Pending(payload, fault))
                    schedule.advance()
                    continue
                if now >= due:  # toca por el reloj monotónico, pero el de pared va atrás
                    waits.append(max(_WALL_SKEW_WAIT_S, ts_epoch - wall))
                else:
                    waits.append(due - now)
        if len(outbox):
            if now >= outbox.retry_at:
                outbox.send_next()
                continue
            waits.append(outbox.retry_at - now)
        clock.wait(max(0.0, min(waits)))

    _drain(outbox, clock, cfg.http_read_timeout_s)
    stats.unsent_at_exit = len(outbox)
    if len(outbox):
        logger.warning(
            "unsent_at_exit",
            extra={
                "event": "unsent_at_exit",
                "station_id": cfg.station_id,
                "buffered": len(outbox),
                "dropped_sequence": outbox.sequences()[:50],
            },
        )
    return stats, exit_reason


def install_signal_handlers(stop: threading.Event) -> None:
    def handler(signum: int, frame: Any) -> None:
        stop.set()  # nada de logging aquí: no es seguro dentro de un manejador de señal

    for name in ("SIGINT", "SIGTERM", "SIGBREAK"):
        sig = getattr(signal, name, None)
        if sig is not None:
            signal.signal(sig, handler)


def main(env: Mapping[str, str] | None = None) -> int:
    configure_logging()
    try:
        cfg = load_config(os.environ if env is None else env)
    except ConfigError as exc:
        logger.error(str(exc), extra={"event": "config_error", "reason": "invalid_config"})
        return 2

    stop = threading.Event()
    install_signal_handlers(stop)
    timeout = httpx.Timeout(cfg.http_read_timeout_s, connect=cfg.http_connect_timeout_s)
    try:
        with httpx.Client(timeout=timeout) as client:
            stats, exit_reason = run(cfg, client, SystemClock(stop), stop)
    except Exception as exc:  # noqa: BLE001 — se registra solo el tipo, nunca el mensaje
        logger.error(
            "station_crashed", extra={"event": "station_crashed", "error_type": type(exc).__name__}
        )
        return 1
    logger.info(
        "summary",
        extra={
            "event": "summary",
            "station_id": cfg.station_id,
            "reason": exit_reason,
            "summary": stats.as_dict(),
        },
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
```

=== station/tests/test_main.py === (277 líneas, sha256: fe545ee7dabfcd14d594e831ae9d460f2c216575058d9d889e49f3731f889009)

```python
import json
import logging
import signal
import threading
from datetime import datetime

import httpx
import pytest

from conftest import START_WALL, FakeApi, FakeClock, LogCapture, make_config
from station.main import DRAIN_TIMEOUT_S, install_signal_handlers, run


def epoch(ts: str) -> float:
    return datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp()


def run_station(api: FakeApi, clock: FakeClock, **overrides: object):
    stats, reason = run(make_config(**overrides), api.client(), clock, clock.stop)
    return stats, reason


def test_live_mode_aligned_timestamps_and_sequence(api: FakeApi, clock: FakeClock) -> None:
    stats, reason = run_station(api, clock, interval_s=60, max_readings=5)
    assert reason == "max_readings"
    floor = int(START_WALL // 60) * 60
    assert [epoch(b["timestamp"]) for b in api.stored] == [floor + 60 * k for k in range(5)]
    assert [b["sequence"] for b in api.stored] == [0, 1, 2, 3, 4]
    assert all(b["timestamp"].endswith("Z") for b in api.stored)
    assert stats.generated == stats.created == 5


def test_live_timestamps_never_in_future(api: FakeApi, clock: FakeClock) -> None:
    sent_at: list[tuple[float, float]] = []
    original = api.handler

    def spy(request: httpx.Request) -> httpx.Response:
        sent_at.append((epoch(json.loads(request.content)["timestamp"]), clock.time()))
        return original(request)

    api.handler = spy  # type: ignore[method-assign]
    run_station(api, clock, interval_s=10, max_readings=30)
    assert len(sent_at) == 30
    assert all(ts <= now for ts, now in sent_at)


def test_no_cumulative_drift_with_slow_requests(api: FakeApi, clock: FakeClock) -> None:
    # Cada solicitud "tarda" 3.7 s; con sleep(intervalo) tras cada envío la serie se iría
    # retrasando. Con el reloj monotónico, en 1 h de 60 s salen exactamente 60 lecturas.
    api.request_delay_s = 3.7
    clock.at(3600, clock.stop.set)
    stats, reason = run_station(api, clock, interval_s=60)
    assert reason == "signal"
    stamps = [epoch(b["timestamp"]) for b in api.stored]
    assert stats.generated == 61  # t0 (piso del arranque) + 60 ticks en la hora
    assert all(b - a == 60 for a, b in zip(stamps, stamps[1:], strict=False))


def test_replay_then_live(api: FakeApi, clock: FakeClock) -> None:
    stats, reason = run_station(
        api, clock, interval_s=60, replay_hours=2, replay_duration_min=0, max_readings=125
    )
    assert reason == "max_readings"
    floor = int(START_WALL // 60) * 60
    stamps = [epoch(b["timestamp"]) for b in api.stored]
    # 120 de réplica (las últimas 2 h, todas <= ahora) y luego modo vivo sin hueco ni repetición.
    assert stamps[:120] == [floor - 60 * (119 - k) for k in range(120)]
    assert all(ts <= START_WALL for ts in stamps[:120])
    assert stamps[120:] == [floor + 60 * k for k in range(1, 6)]
    assert [b["sequence"] for b in api.stored] == list(range(125))
    assert stats.created == 125


def test_replay_is_spread_over_duration(api: FakeApi, clock: FakeClock) -> None:
    sent_mono: list[float] = []
    original = api.handler

    def spy(request: httpx.Request) -> httpx.Response:
        sent_mono.append(clock.monotonic())
        return original(request)

    api.handler = spy  # type: ignore[method-assign]
    run_station(api, clock, interval_s=60, replay_hours=1, replay_duration_min=2, max_readings=60)
    assert len(sent_mono) == 60
    elapsed = sent_mono[-1] - sent_mono[0]
    assert elapsed == pytest.approx(120 * 59 / 60)


def test_live_wall_clock_jump_forward_reanchors(
    api: FakeApi, clock: FakeClock, logs: LogCapture
) -> None:
    # Equipo suspendido: el reloj de pared salta +8 h sin que avance el monotónico. La
    # estación no rellena el hueco (suspendida no mide): reancla al reloj de pared y sigue.
    clock.at(120, lambda: setattr(clock, "wall", clock.wall + 8 * 3600))
    clock.at(900, clock.stop.set)
    stats, reason = run_station(api, clock, interval_s=60)
    assert reason == "signal"
    stamps = [epoch(b["timestamp"]) for b in api.stored]
    floor = int(START_WALL // 60) * 60
    assert all(ts % 60 == 0 for ts in stamps)
    assert all(b > a for a, b in zip(stamps, stamps[1:], strict=False))  # sin repetidos
    assert [b["sequence"] for b in api.stored] == list(range(len(stamps)))
    assert clock.time() - stamps[-1] < 60  # atraso final menor a un intervalo
    # La espera en curso (tick de 06:03, a los 143 s) termina tras el salto; ahí se detecta.
    # Último generado 06:02; siguiente: piso de 14:03:00 -> se omiten 06:03..14:02 (480).
    [warning] = logs.events("clock_jump")
    assert warning.levelno == logging.WARNING
    assert warning.ticks_skipped == 480
    assert warning.jump_s == pytest.approx(8 * 3600, abs=1)
    assert stamps[:3] == [floor, floor + 60, floor + 120]
    assert stamps[3] == floor + 8 * 3600 + 180
    gaps = [b - a for a, b in zip(stamps, stamps[1:], strict=False)]
    assert gaps.count(60) == len(gaps) - 1 and max(gaps) == (480 + 1) * 60


def test_small_wall_clock_jump_does_not_reanchor(
    api: FakeApi, clock: FakeClock, logs: LogCapture
) -> None:
    # +100 s < 2 x 60 s: no se reancla, la serie sigue contigua.
    clock.at(120, lambda: setattr(clock, "wall", clock.wall + 100))
    clock.at(900, clock.stop.set)
    run_station(api, clock, interval_s=60)
    stamps = [epoch(b["timestamp"]) for b in api.stored]
    assert logs.events("clock_jump") == []
    assert all(b - a == 60 for a, b in zip(stamps, stamps[1:], strict=False))


def test_backward_wall_clock_jump_waits_without_repeating(
    api: FakeApi, clock: FakeClock, logs: LogCapture
) -> None:
    # -10 min: no se reancla (repetiría timestamps ya guardados y el modelo exige tiempo
    # creciente). La estación espera a que el reloj de pared alcance el siguiente timestamp.
    generated_at: list[tuple[float, float]] = []  # (monotónico, timestamp) de cada envío
    original = api.handler

    def spy(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        generated_at.append((clock.monotonic() - 1000.0, epoch(body["timestamp"])))
        assert epoch(body["timestamp"]) <= clock.time()  # nunca en el futuro
        return original(request)

    api.handler = spy  # type: ignore[method-assign]
    clock.at(120, lambda: setattr(clock, "wall", clock.wall - 600))
    clock.at(1200, clock.stop.set)
    run_station(api, clock, interval_s=60)
    stamps = [ts for _, ts in generated_at]
    floor = int(START_WALL // 60) * 60
    assert logs.events("clock_jump") == []
    assert all(b - a == 60 for a, b in zip(stamps, stamps[1:], strict=False))
    assert [b["sequence"] for b in api.stored] == list(range(len(stamps)))
    # Tras el salto (pared 05:52:37 a los 120 s), el tick de 06:03 espera hasta los 743 s.
    assert [mono for mono, ts in generated_at if ts == floor + 180] == [pytest.approx(743)]
    assert not [mono for mono, _ in generated_at if 120 < mono < 743]


def test_wall_clock_jump_during_replay_reanchors_only_when_live_starts(
    api: FakeApi, clock: FakeClock, logs: LogCapture
) -> None:
    # Réplica de 1 h repartida en 10 min (una lectura cada 10 s). Salto +8 h a los 300 s:
    # la réplica conserva sus timestamps (todos <= arranque); el reanclaje ocurre al pasar
    # al modo vivo, a los 590 s (última lectura de la réplica).
    clock.at(300, lambda: setattr(clock, "wall", clock.wall + 8 * 3600))
    clock.at(900, clock.stop.set)
    run_station(api, clock, interval_s=60, replay_hours=1, replay_duration_min=10)
    stamps = [epoch(b["timestamp"]) for b in api.stored]
    floor = int(START_WALL // 60) * 60
    assert stamps[:60] == [floor - 60 * (59 - k) for k in range(60)]
    [warning] = logs.events("clock_jump")
    first_live = int((START_WALL + 590 + 8 * 3600) // 60) * 60
    assert stamps[60] == first_live
    assert warning.ticks_skipped == (first_live - (floor + 60)) // 60
    assert all(b > a for a, b in zip(stamps, stamps[1:], strict=False))
    assert [b["sequence"] for b in api.stored] == list(range(len(stamps)))


def test_fast_replay_larger_than_buffer_does_not_overflow(api: FakeApi, clock: FakeClock) -> None:
    # Regresión (hallada en la verificación real): una réplica de 24 h con
    # REPLAY_DURATION_MIN=0 llenaba BUFFER_MAX=1000 y descartaba las 440 más viejas.
    stats, reason = run_station(
        api, clock, interval_s=60, replay_hours=24, replay_duration_min=0, max_readings=1440,
        buffer_max=1000,
    )
    assert reason == "max_readings"
    assert [b["sequence"] for b in api.stored] == list(range(1440))
    assert stats.dropped_overflow == 0 and stats.created == 1440 and stats.unsent_at_exit == 0


def test_replay_pauses_while_api_is_down(api: FakeApi, clock: FakeClock) -> None:
    clock.at(0.5, lambda: setattr(api, "down", True))
    clock.at(120, lambda: setattr(api, "down", False))
    api.request_delay_s = 0.01
    stats, _ = run_station(
        api, clock, interval_s=60, replay_hours=2, replay_duration_min=0, max_readings=130,
        buffer_max=5,
    )
    assert stats.dropped_overflow == 0
    assert [b["sequence"] for b in api.stored] == list(range(130))


def test_max_readings_waits_for_delivery(api: FakeApi, clock: FakeClock) -> None:
    # La última lectura se genera con el API caído: la estación no sale hasta entregarla
    # (no hay tope de 10 s en este caso; ese tope es solo para el cierre por señal).
    clock.at(40, lambda: setattr(api, "down", True))
    clock.at(40 + 90, lambda: setattr(api, "down", False))
    stats, reason = run_station(api, clock, interval_s=10, max_readings=6)
    assert reason == "max_readings"
    assert [b["sequence"] for b in api.stored] == list(range(6))
    assert stats.retries > 0 and stats.unsent_at_exit == 0


def test_outage_buffers_and_delivers_in_order(api: FakeApi, clock: FakeClock) -> None:
    clock.at(25, lambda: setattr(api, "down", True))
    clock.at(25 + 45, lambda: setattr(api, "down", False))
    stats, reason = run_station(api, clock, interval_s=10, max_readings=20)
    sequences = [b["sequence"] for b in api.stored]
    assert sequences == list(range(20))
    assert len({b["timestamp"] for b in api.stored}) == 20
    assert stats.retries > 0 and stats.created == 20 and stats.unsent_at_exit == 0


def test_signal_stop_drains_and_exits(api: FakeApi, clock: FakeClock, logs: LogCapture) -> None:
    # Arranque a :37 con intervalo 10: ticks en +0 (:30), +3, +13, +23 y +33 s.
    clock.at(35, clock.stop.set)
    stats, reason = run_station(api, clock, interval_s=10)
    assert reason == "signal"
    assert stats.generated == 5 and stats.created == 5 and stats.unsent_at_exit == 0


def test_drain_gives_up_after_timeout(api: FakeApi, clock: FakeClock, logs: LogCapture) -> None:
    api.down = True
    clock.at(35, clock.stop.set)
    start = clock.monotonic()
    stats, reason = run_station(api, clock, interval_s=10)
    assert reason == "signal"
    assert stats.created == 0 and stats.unsent_at_exit == 5
    assert clock.monotonic() - (start + 35) <= DRAIN_TIMEOUT_S + 1e-6
    [warning] = logs.events("unsent_at_exit")
    assert warning.dropped_sequence == [0, 1, 2, 3, 4]


def test_faults_create_sequence_gaps(api: FakeApi, clock: FakeClock, logs: LogCapture) -> None:
    # El API falso no valida, así que acepta las fallas: aquí solo se cuenta qué se inyectó.
    stats, _ = run_station(
        api, clock, interval_s=60, replay_hours=1, replay_duration_min=0, max_readings=60,
        fault_rate=0.3,
    )
    injected = {r.sequence for r in logs.events("fault_injected")}
    assert stats.faults_injected == len(injected) > 0
    assert [b["sequence"] for b in api.requests] == list(range(60))


def test_fault_rate_does_not_change_weather_end_to_end(clock: FakeClock, logs: LogCapture) -> None:
    clean_api, faulty_api = FakeApi(), FakeApi()
    common = {"replay_hours": 2, "replay_duration_min": 0, "max_readings": 120}
    run(make_config(**common), clean_api.client(), FakeClock(), threading.Event())
    run(make_config(**common, fault_rate=0.3), faulty_api.client(), FakeClock(), threading.Event())
    mutated = {r.sequence for r in logs.events("fault_injected")}
    assert 0 < len(mutated) < 120
    clean = {b["sequence"]: b for b in clean_api.requests}
    for body in faulty_api.requests:
        if body["sequence"] not in mutated:
            assert body == clean[body["sequence"]]


def test_install_signal_handlers_sets_event() -> None:
    stop = threading.Event()
    names = [n for n in ("SIGINT", "SIGTERM", "SIGBREAK") if hasattr(signal, n)]
    previous = {n: signal.getsignal(getattr(signal, n)) for n in names}
    try:
        install_signal_handlers(stop)
        signal.raise_signal(signal.SIGINT)
        assert stop.is_set()
        for name in names:
            assert callable(signal.getsignal(getattr(signal, name)))
    finally:
        for name, handler in previous.items():
            signal.signal(getattr(signal, name), handler)
```

=== docs/adr/0002-estacion-simulada.md === (168 líneas, sha256: 387a93a280efcc2f050f57dea15220f65e47a5b6f9a28ece3ced680b635b69be)

```markdown
# ADR 0002 — Estación meteorológica simulada (generador y emisor)

- **Estado:** Aceptada
- **Fecha:** 2026-10-01
- **Alcance:** Fase B. Generador, inyección de fallas y emisor en Python 3.13, ejecutados con el venv local contra uvicorn real. Sin Docker (Fase C) ni diagramas (Fase D).

## Contexto

El API de ingesta (ADR 0001) existe y valida el contrato v1 (`ingest_api/app/models.py`, fuente de verdad). Falta la estación: un proceso por estación (GDL-001, GDL-002, GDL-003) que genere lecturas físicamente coherentes con la climatología de Guadalajara, las envíe con su clave y sobreviva a caídas del API sin perder ni duplicar datos.

## Estructura

| Ruta | Contenido |
|---|---|
| `station/station/config.py` | Variables de entorno, validación fail-fast (código de salida 2), `Secret` para la clave |
| `station/station/solar.py` | Declinación, ecuación del tiempo, elevación solar, radiación de cielo despejado |
| `station/station/weather_model.py` | `WeatherModel.step(ts)` con estado y `ReadingGenerator` (payload v1, `sequence`) |
| `station/station/faults.py` | Catálogo de 9 fallas y `FaultInjector` con su propio generador aleatorio |
| `station/station/sender.py` | `Sender` (un POST, clasificación de la respuesta) y `Outbox` (búfer FIFO, backoff, contadores) |
| `station/station/main.py` | Programación de ticks (vivo y réplica), señales, vaciado final y resumen |
| `station/station/log.py` | Logs JSON de una línea, ASCII escapado (mismo formato que el API) |
| `station/tests/` | 5 archivos de prueba; `test_main.py` se añadió a la lista acordada para separar las pruebas del bucle (ticks, réplica, señales) de las del emisor |
| `contract_tests/` | Salida del generador contra el modelo real del API, con su propio `pytest.ini` |

Cada suite se corre desde su carpeta (`station/`, `ingest_api/`, `contract_tests/`): `station/tests` e `ingest_api/tests` chocarían como paquete `tests` en una misma sesión.

**Dependencias:** `httpx==0.28.1` y sus transitivas, todas con versión exacta. Conviven en el mismo venv con `ingest_api/requirements-dev.txt`. Starlette usa `httpx2` para su `TestClient` y solo recurre a `httpx` si `httpx2` falta. Son módulos distintos (`httpx` y `httpx2`), así que no hay conflicto. Se comprobó con `pip install --dry-run` de ambos archivos juntos, `pip check` y la suite del API con `-W error::DeprecationWarning`.

## Decisión 1 — Modelo meteorológico

Climatología mensual aproximada de GDL (Tmax, Tmin, punto de rocío, probabilidad diaria de lluvia y nubosidad media), interpolada por día. Conviene contrastarla con las normales del SMN si se requiere rigor. La hora local usa un offset fijo, `TZ_OFFSET_H=-6`: México no usa horario de verano desde 2022, así que no se necesita `zoneinfo` ni `tzdata`.

| Variable | Resolución | Rango simulado (GDL) | Validación del API | Modelo |
|---|---|---|---|---|
| `air_temperature_c` | 0.1 °C | ~5–35 | −40 a 60 | Media estacional + anomalía día a día + ciclo diurno asimétrico (mínimo al amanecer + 0.15 h, máximo al mediodía solar + 2.2 h) + ruido AR(1) − enfriamiento por lluvia (3–6 °C). La amplitud baja hasta 45 % con nubes |
| Punto de rocío (interno) | — | ~0–17 | no se envía | AR(1) lento (τ = 12 h). La lluvia lo acerca a T; siempre ≤ T. Solo sirve para calcular la HR |
| `relative_humidity_pct` | 1 % (entero) | ~15–100 | 0–100 | Fórmula de Magnus a partir de T y del punto de rocío; ≥ 90 % con lluvia |
| `station_pressure_hpa` | 0.1 hPa | ~836–846 | 500–1100 | Fórmula barométrica (840.4 hPa a 1,550 m) + marea semidiurna ±1.2 hPa (máximos a las 10 y 22 h) + deriva sinóptica AR(1) (τ = 2 días, acotada a ±3.5 hPa) |
| `wind_speed_ms` | 0.1 m/s | 0–10 | 0–75 | Weibull (k = 2) con autocorrelación: AR(1) gaussiano transformado por cuantil. Media de 1.0 m/s (~04 h) a 4.2 m/s (~16 h) |
| `wind_gust_ms` | 0.1 m/s | 0–20 | 0–100, ≥ velocidad | Velocidad × (1.3 + 0.1·log10(dt/10) + U(0, 0.25)), +0.4 al inicio de una tormenta |
| `wind_direction_deg` | 1° (entero) | 0–359 | 0–359 o null | Dirección predominante (`PREVAILING_WIND_DEG`, 250°) + desviación AR(1) lenta + ruido AR(1); `null` si la velocidad redondeada es < 0.5 m/s |
| `precipitation_mm` | 0.2 mm | 0 a ~10 por intervalo | 0–200 | Cadena de Markov en tiempo continuo. Inicios con tasa de Poisson, 75 % entre 16 y 22 h. Duración media 1.5 h. Intensidad lognormal (mediana 3 mm/h, tope 60 mm/h). Pluviómetro de balancín |
| `solar_radiation_wm2` | 1 W/m² (entero) | 0–~1,130 | 0–1500 | Cielo despejado (Haurwitz + 6 %/km de altitud) × (1 − 0.75·nubes^3.4), × 0.3 con lluvia; 0 con el sol bajo el horizonte |
| `device.battery_v` | 0.01 V | 12.0–13.8 | 0–15 | Carga solar hacia 13.8 V y descarga hacia 12.0 V, con integración exacta en dt |

**Coherencia con cualquier `INTERVAL_S` (10–3600 s):**

- Cada proceso AR(1) usa `phi = exp(-dt/tau)` con innovación `sigma·sqrt(1 − phi²)`. La varianza marginal y la autocorrelación en tiempo físico no dependen del intervalo; hay una prueba para dt = 10, 60, 300 y 3600.
- Los inicios y finales de lluvia usan probabilidades `1 − exp(−tasa·dt)`.
- **Precipitación por intervalo:** el balancín acumula `tasa·dt` y reporta las vueltas completas de 0.2 mm de cada intervalo. El resto queda para el siguiente. Una lluvia constante de 7.3 mm/h durante 3 h suma lo mismo, con diferencia menor a 0.2 mm, a 10, 60, 300 y 3600 s.
- **Inercia térmica:** la temperatura responde a la nubosidad y a la anomalía filtradas (paso bajo, τ = 3 h). Esto se añadió tras la primera medición: con la nubosidad instantánea, un claro a media tarde movía Tmax hasta las 17.5 h en ~10 % de los días secos.

**Redondeo sin artefactos de flotante:** primero se cuenta un entero (vueltas del balancín) y luego se aplica `round(vueltas × 0.2, 1)`. `json.dumps(3*0.2)` da `0.6000000000000001`; el generador da `0.6`. Además, `+ 0.0` convierte `-0.0` en `0.0`. Los campos enteros (`interval_s`, `sequence`, `wind_direction_deg`, HR, radiación) son `int` de Python y se serializan sin `.0`.

**`sequence`** empieza en 0 y sube en cada lectura generada, también en las que luego se mutan con una falla. Los huecos en el JSONL del API son exactamente las fallas.

**Timestamps:** son múltiplos del intervalo (piso del reloj de pared), en formato RFC 3339 UTC con `Z` a segundos, y nunca quedan en el futuro.

## Decisión 2 — Semillas y reproducibilidad

- `SEED` por defecto = los primeros 8 bytes de `sha256(STATION_ID)` como entero. No se usa `hash()`, que cambia en cada proceso. Así, cada estación tiene su serie y es estable entre ejecuciones y plataformas.
- El clima usa un `random.Random(SEED)` propio. Las fallas usan **otro** `random.Random`, sembrado con `sha256("faults:{SEED}")`. Cambiar `FAULT_RATE` no altera los valores meteorológicos de las lecturas que no se mutan (probado comparando el JSON byte a byte con 0 y 0.3).
- Misma semilla, mismo intervalo y mismo instante de arranque → la misma serie byte a byte (probado con sha256 del flujo de payloads).
- **Ojo:** con la misma `SEED` explícita en dos estaciones, las series son idénticas. Es lo esperado de una semilla reproducible.
- **Límite:** el resultado depende de `math` (sin, exp…) en el último bit. En otra plataforma o versión de Python, un valor podría cambiar en el redondeo. No se verificó fuera de Windows con Python 3.13.5.

## Decisión 3 — Política de respuestas, reintentos y búfer

| Respuesta | Clasificación | Acción |
|---|---|---|
| 2xx (201) | entregada (`created`) | Sale del búfer |
| 409 | entregada (`duplicates`) | Sale del búfer; ya estaba guardada, p. ej. si se perdió la respuesta del primer intento |
| 5xx, 408, 429, timeout, error de conexión o de protocolo | reintentable | Se queda al frente del búfer. Backoff `min(60, 2^(n−1)) × U(0.5, 1)` s; se reinicia tras cualquier resultado definitivo |
| 400, 413, 415, 422 y otros 4xx | no reintentable | Se descarta y se cuenta en `dropped_by_status`. Log WARNING con `status`, `problem_type` y los pares `[field, code]` del problem+json (sin `value` ni payload) |
| 401 | no reintentable | Se descarta. **ERROR** con `reason: check_station_key`: "el API no reconoce STATION_KEY; revisa la clave (y que su hash esté en STATION_KEY_HASHES del API)" |
| 403 | no reintentable | Se descarta. **ERROR** con `reason: check_station_id`: "la clave pertenece a otra estación; revisa STATION_ID y STATION_KEY" |

**401/403 no detienen la estación:** cada lectura nueva hace un solo intento y se descarta. Así queda un ERROR por intervalo, sin bucle de reintentos. Si la configuración del API se corrige, las lecturas siguientes se aceptan sin reiniciar la estación.

**Búfer:**
- En memoria, FIFO, acotado a `BUFFER_MAX`.
- Solo se envía el frente, así que el orden de entrega se conserva.
- Si se llena, se descarta la lectura **más vieja**, con contador (`dropped_overflow`) y WARNING (`buffer_overflow`).

**Programación de ticks:**
- **Modo vivo:** se programa sobre el reloj monotónico desde el arranque (`instante = inicio + (ts − arranque)`), no con `sleep(intervalo)` tras cada envío. No hay deriva acumulada aunque las solicitudes tarden (probado con solicitudes de 3.7 s: 60 lecturas exactas por hora). Los ticks atrasados se generan todos, cada uno con su timestamp.
- **Salto del reloj de pared hacia adelante (solo modo vivo):** se calcula la deriva `pared − (pared_ancla + (monotónico − monotónico_ancla))`.
  - **Motivo:** si el equipo se suspende, el reloj de pared avanza y el monotónico puede no hacerlo. Sin corrección, la serie quedaba atrasada para siempre: con un salto de +8 h, el último timestamp iba 8.01 h detrás del reloj de pared y seguía separado 60 s.
  - **Reanclaje:** si la deriva supera `max(2 × INTERVAL_S, 5 s)`, el calendario se reancla al instante actual. El siguiente timestamp es el piso del reloj de pared de ahora, siempre estrictamente mayor que el último generado.
  - **Ticks perdidos:** se omiten, no se rellenan, porque una estación suspendida no mide. `sequence` sigue contigua, pues solo cuenta lecturas generadas. Los huecos de timestamp en el JSONL son el tiempo suspendido.
  - **Aviso:** se registra un WARNING `clock_jump` con `jump_s` (deriva) y `ticks_skipped` (timestamps alineados que no se generaron).
  - **Saltos pequeños:** uno menor que el umbral no se reancla y la serie sigue contigua, con ese atraso.
- **Salto del reloj de pared hacia atrás:** no se corrige. El modelo exige tiempo creciente, y reanclar repetiría timestamps ya guardados, que darían 409. La estación espera a que el reloj de pared alcance el siguiente timestamp; no genera nada mientras tanto y nunca envía un timestamp en el futuro. Con −10 min, el tick siguiente esperó esos 10 min.
- **Espera:** usa `Event.wait` en tramos de 0.5 s para atender señales pronto, también en Windows.

**Fin:**
- Con `MAX_READINGS`, la estación termina cuando entregó o descartó todo lo generado.
- Con SIGINT, SIGTERM o SIGBREAK, se activa un `Event`, se intenta vaciar el búfer durante 10 s como máximo y se registra lo que quede (`unsent_at_exit`).
- En ambos casos imprime un resumen JSON (`event: summary`) y sale con código 0.
- El manejador de señal no escribe logs: no es seguro dentro de un manejador.

## Decisión 4 — Modo réplica

- Con `REPLAY_HOURS = N > 0`, al arrancar se generan las lecturas de las últimas N horas: `N·3600 // INTERVAL_S` timestamps alineados, el último igual al piso del arranque, todos ≤ ahora.
- Se envían repartidas en `REPLAY_DURATION_MIN` (0 = lo más rápido posible).
- Después sigue el modo vivo con el mismo modelo y la misma `sequence`, sin hueco ni repetición de timestamps.
- `REPLAY_HOURS` tiene un máximo de 167: el API rechaza timestamps de más de 7 días (168 h) y queda 1 h de margen para enviar.
- **Contrapresión:** la siguiente lectura de réplica se genera solo cuando el búfer está vacío. Las lecturas en vivo no esperan, porque dependen del reloj. Si el API cae a mitad de una réplica, la réplica se pausa en lugar de desbordar el búfer.
- **Salto del reloj durante la réplica:** no se reancla. La réplica conserva sus timestamps, todos fijados al arrancar y ≤ el arranque. El salto se detecta y se reancla en el primer tick vivo, con el mismo WARNING `clock_jump`. Así, el modo vivo empieza en el piso del reloj de pared de ese momento, y no en arranque + intervalo con horas de atraso.

## Decisión 5 — Catálogo de fallas

Con probabilidad `FAULT_RATE` por lectura se aplica **una** mutación sobre una copia del payload ya generado. Las 9 dan 422 con exactamente un error:

| Falla | Mutación | `field` esperado | `code` esperado |
|---|---|---|---|
| `humidity_over_100` | HR = 101 | `measurements.relative_humidity_pct` | `less_than_equal` |
| `temperature_below_min` | T = −41 | `measurements.air_temperature_c` | `greater_than_equal` |
| `gust_below_speed` | velocidad = máx(velocidad, 1); ráfaga = velocidad − 0.5 | `measurements.wind_gust_ms` | `gust_less_than_speed` |
| `extra_field` | `"unexpected_field": 1` en la raíz | `unexpected_field` | `extra_forbidden` |
| `missing_required` | sin `station_pressure_hpa` | `measurements.station_pressure_hpa` | `missing` |
| `string_in_number` | T como texto (`"24.7"`) | `measurements.air_temperature_c` | `float_type` |
| `future_timestamp` | **reloj actual** + 10 min | `timestamp` | `timestamp_in_future` |
| `bad_schema_version` | `"2.0"` | `schema_version` | `literal_error` |
| `float_in_integer` | `interval_s` como flotante (60 → 60.0) | `interval_s` | `int_type` |

**Comprobación:** el emisor compara la respuesta real con lo esperado. Si el 422 trae exactamente ese par (field, code), registra `fault_confirmed` (INFO). Si no (otro código, otro status o un 201), registra `fault_mismatch` (WARNING) con `expected` y `received`. Los contadores van al resumen: `faults_injected`, `faults_confirmed` y `faults_unexpected`.

**Ajuste respecto al planteamiento:**
- `future_timestamp` usa **reloj actual + 10 min**, no timestamp de la lectura + 10 min. En réplica, o con intervalos largos (p. ej. 3600 s), el timestamp + 10 min sigue en el pasado y el API lo aceptaría.
- `float_in_integer` usa `float(INTERVAL_S)` y no siempre `60.0`, para no cambiar el intervalo de la lectura.

## Seguridad de la clave

- `STATION_KEY` se guarda en un `Secret` cuyo `repr`, `str` y f-string muestran `***`. El dataclass `Config` la excluye de su `repr`.
- Los errores de configuración no reflejan **ningún** valor, por si alguien pega la clave en otra variable.
- Los logs nunca llevan el payload completo ni los `value` del problem+json.
- Las excepciones de transporte se registran solo por su tipo.

## Verificación (resumen; evidencia completa en el reporte de la fase)

- **Pruebas:** station 155 (151 + 4 del salto de reloj), ingest_api 98, contract_tests 12. El contrato valida 4,320 payloads (24 h × 3 estaciones) contra el `WeatherReading` real, y cada falla devuelve exactamente su (field, code).
- **Procesos reales, sin Docker:** se comprobaron estos puntos contra uvicorn real.
  - 3 estaciones en vivo (18 líneas únicas).
  - Réplica de 2 h con repetición: 409 contados como `duplicates`; líneas nuevas = timestamps nuevos.
  - Fallas con `FAULT_RATE=0.3`: todas confirmadas y 0 `fault_mismatch`; los huecos coinciden con las fallas.
  - Caída del API de 50 s: entrega en orden, sin pérdidas ni duplicados.
  - 401 y 403 sin reintentos.
  - Cierre con `CTRL_BREAK_EVENT`.
  - Búsqueda de las 3 claves en claro: 0 coincidencias.
- **Bug hallado y corregido durante la verificación:**
  - **Síntoma:** una réplica de 24 h con `REPLAY_DURATION_MIN=0` envió 549 de 1,440 lecturas.
  - **Causa:** el bucle generaba todas las lecturas vencidas antes de enviar, así que desbordó `BUFFER_MAX=1000` y descartó las 440 más viejas. Además, `MAX_READINGS` cortaba al generar la última, y el vaciado de 10 s no alcanzó.
  - **Corrección:** contrapresión en la réplica, y `MAX_READINGS` termina al entregar.
  - **Pruebas de regresión:** 3 en `test_main.py`.

## Límites conocidos y lo que no se verificó

- **Suspensión real del equipo:** no se verificó una suspensión real del equipo ni de Docker Desktop. El salto del reloj de pared solo se simuló con el reloj falso de las pruebas (+8 h, +100 s, −10 min y +8 h durante una réplica). Tampoco se midió si en Windows `time.monotonic()` avanza o no durante la suspensión. Si avanzara, no habría deriva y la estación generaría de golpe los ticks atrasados, como en cualquier atraso.
- **SIGINT real en Windows:** se demostró el cierre ordenado con `CTRL_BREAK_EVENT` (SIGBREAK) sobre un proceso real en su propio grupo de consola. SIGINT (`CTRL_C_EVENT`) no se envió a un proceso real: en Windows, `CTRL_C_EVENT` llega a todo el grupo de consola, incluida la terminal que lo envía. SIGINT sí se probó en el mismo proceso con `signal.raise_signal`. SIGTERM no se probó en un proceso real: en Windows, `Popen.terminate()` es `TerminateProcess`, que no entrega ninguna señal.
- **El búfer vive en memoria:** si el proceso muere (kill, corte de luz), se pierde lo pendiente. El vaciado de 10 s solo ocurre con un cierre ordenado.
- **401/403:** cada lectura afectada se pierde (se descarta por diseño) hasta corregir la clave o el `station_id`.
- **`future_timestamp` puede no dar 422:** pasa si la lectura espera en el búfer más de 5 min (caída del API) o si el API tiene `MAX_FUTURE_SKEW_S` ≥ 600. Si el API la aceptara, ocuparía un timestamp futuro, y la lectura real de ese instante recibiría 409. El emisor lo registraría como `fault_mismatch`. No ocurrió en la verificación.
- **No se honra `Retry-After`** en 429/503; el API actual no lo envía.
- **Rangos simulados aproximados:** en la prueba de 20 semillas, la mínima simulada en enero llegó a 4.8 °C, ligeramente bajo el "~5" de la tabla. La radiación máxima de cielo despejado en enero es ~830 W/m² (sol más bajo), así que el rango 900–1,200 se cumple en abril y octubre, las fechas de prueba, no todo el año. En julio, con 13–15 de 20 días con lluvia, hay pocos días secos para medir Tmax y Tmin; esas propiedades se prueban en abril y octubre.
- **Velocidad media con intervalos largos:** con `INTERVAL_S=3600`, la velocidad reportada es un valor del proceso en ese instante, no un promedio de la hora. Solo la ráfaga crece con el intervalo.
- **Rendimiento de las pruebas:** `test_weather_model.py` simula 415,000 pasos (20 semillas × 2 fechas × 3 intervalos). En esta máquina, con el CPU limitado por el sistema durante la sesión (un bucle de referencia tardó entre 0.38 y 5.9 s), la suite completa tardó 11 min 35 s; sin esa limitación tardó 67 s. Casi todo el tiempo es la corrida a 10 s.
- **Fuera de alcance de esta fase:** Docker y compose (Fase C), diagramas C4 (Fase D) y Azure.
```

=== README.md === (117 líneas, sha256: e202b4936c50c930b0df40d761c9df093fae4fd11d21d4bc077fcacccec9573b)

````markdown
# Skilliket — Estación meteorológica simulada

Prototipo del TEC de Monterrey GDA. Una estación meteorológica emulada envía lecturas a un API REST que las valida y las guarda. Esta entrega corre solo en local; el objetivo en Azure se describe en `docs/adr/` y en el diagrama C4 (fase posterior).

## Estado

- **Fase A — cerrada y verificada:** contrato v1 (`schema/`) y API de ingesta (`ingest_api/`), 98 pruebas en verde y verificación contra uvicorn real. El registro del trabajo (propuesta, reportes y volcados de revisión) está en [`docs/historial/`](docs/historial/).
- **Fase B — terminada:** estación simulada (`station/`). Incluye el modelo meteorológico de GDL, el emisor con reintentos y búfer, el modo réplica y la inyección de fallas. Pruebas en verde en `station/`, `ingest_api/` y `contract_tests/`, y verificación con uvicorn y 3 procesos de estación reales. Decisiones en [`docs/adr/0002-estacion-simulada.md`](docs/adr/0002-estacion-simulada.md).
- **Pendiente:** Docker/compose (Fase C) y diagramas C4 (Fase D).

## Estructura

| Ruta | Contenido |
|---|---|
| `ingest_api/app/` | API FastAPI: modelo, autenticación, errores, almacenamiento |
| `ingest_api/tests/` | Pruebas del API (pytest) |
| `station/station/` | Estación simulada: configuración, modelo meteorológico, fallas, emisor |
| `station/tests/` | Pruebas de la estación (pytest) |
| `contract_tests/` | Contrato: salida del generador contra el modelo real del API |
| `schema/weather_reading.v1.schema.json` | JSON Schema exportado del modelo Pydantic |
| `scripts/gen_station_key.py` | Genera una clave de estación y su hash SHA-256 |
| `scripts/export_schema.py` | Regenera el JSON Schema |
| `docs/adr/` | Decisiones de arquitectura |

## Ejecutar en local

Requiere Python 3.13.

```powershell
py -3.13 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r ingest_api\requirements-dev.txt -r station\requirements-dev.txt

# Una clave por línea de salida: guarda STATION_KEY para la estación y usa SHA256 en el API.
.\.venv\Scripts\python.exe scripts\gen_station_key.py

$env:STATION_KEY_HASHES = '{"GDL-001": ["<sha256>"]}'
cd ingest_api
..\.venv\Scripts\python.exe -m uvicorn app.main:create_app --factory --port 8000
```

- Documentación interactiva: http://127.0.0.1:8000/docs
- Variables de entorno del API: ver `.env.example`.

### Tres estaciones

Genera una clave por estación (`scripts\gen_station_key.py`, tres veces). Arranca el API con los tres hashes:

```powershell
$env:STATION_KEY_HASHES = '{"GDL-001": ["<sha256-1>"], "GDL-002": ["<sha256-2>"], "GDL-003": ["<sha256-3>"]}'
```

Luego abre una terminal por estación y entrega a cada una solo su clave:

```powershell
cd station
$env:STATION_ID = "GDL-001"          # GDL-002 / GDL-003 en las otras terminales
$env:STATION_KEY = "<clave en claro de GDL-001>"
..\.venv\Scripts\python.exe -m station.main
```

- **Detener:** Ctrl+C. La estación intenta vaciar el búfer durante 10 s como máximo e imprime un resumen JSON (`event: summary`). Ctrl+C se probó solo a nivel del manejador de señales; el cierre real de un proceso se demostró con CTRL_BREAK (ver ADR 0002).
- **Demo rápida:** `$env:REPLAY_HOURS = "2"; $env:REPLAY_DURATION_MIN = "0"` envía las últimas 2 h al arrancar y sigue en vivo.
- **Demo de 422:** `$env:FAULT_RATE = "0.3"`. Cada falla se registra como `fault_confirmed` o `fault_mismatch`.
- **Configuración inválida:** la estación no arranca, muestra el motivo (nunca la clave) y sale con código 2.

#### Variables de entorno de la estación

Todas están en [`station/.env.example`](station/.env.example), con sus valores por defecto.

| Variable | Defecto | Valores |
|---|---|---|
| `STATION_ID` | — (obligatoria) | `^[A-Z]{3}-[0-9]{3}$` |
| `STATION_KEY` | — (obligatoria, secreto) | 16–256 caracteres ASCII visibles, sin espacios |
| `INGEST_URL` | `http://127.0.0.1:8000/v1/readings` | URL http(s), sin credenciales |
| `INTERVAL_S` | `60` | Entero de 10 a 3600 |
| `SEED` | Entero derivado de `sha256(STATION_ID)` | Entero ≥ 0 |
| `LAT`, `LON` | `20.67`, `-103.35` | −90 a 90, −180 a 180 |
| `ELEVATION_M` | `1550` | −400 a 5000 |
| `TZ_OFFSET_H` | `-6` | −12 a 14 (offset fijo, sin horario de verano) |
| `PREVAILING_WIND_DEG` | `250` | Entero de 0 a 359 |
| `REPLAY_HOURS` | `0` (apagado) | Entero de 0 a 167 |
| `REPLAY_DURATION_MIN` | `2` | 0 (lo más rápido posible) a 1440 |
| `FAULT_RATE` | `0` | 0.0 a 1.0 |
| `BUFFER_MAX` | `1000` | Entero de 1 a 1,000,000; al llenarse se descarta la lectura más vieja |
| `MAX_READINGS` | `0` (sin límite) | Entero ≥ 0; termina al entregar o descartar esa cantidad |
| `HTTP_CONNECT_TIMEOUT_S`, `HTTP_READ_TIMEOUT_S` | `5`, `10` | Mayor que 0, máximo 120 |

## Pruebas

Cada suite se corre desde su carpeta: `ingest_api/tests` y `station/tests` chocarían como paquete `tests` en una misma sesión.

```powershell
cd ingest_api;     ..\.venv\Scripts\python.exe -m pytest; cd ..
cd station;        ..\.venv\Scripts\python.exe -m pytest; cd ..
cd contract_tests; ..\.venv\Scripts\python.exe -m pytest; cd ..
```

`station/tests/test_weather_model.py` simula 24 h con 20 semillas, 2 fechas e intervalos de 60, 10 y 300 s: tarda alrededor de 1 min en un equipo normal (51–67 s medidos; más si el CPU está limitado).

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

=== docs/historial/README.md === (15 líneas, sha256: 2d177b79fafc750619bd1850445b87221eff8d8b3f73479985bef3239c5b7eaf)

```markdown
# Historial de trabajo

Documentos de trabajo, en orden cronológico. Como el repositorio es solo local y no tiene commits, son el registro de cómo se llegó al estado actual. Las decisiones vigentes están en [`docs/adr/`](../adr/).

| Archivo | Qué es | Fase |
|---|---|---|
| [propuesta-diseno-estacion-meteorologica.md](propuesta-diseno-estacion-meteorologica.md) | Investigación inicial y propuesta de diseño: ingesta, framework, variables, esquema, fases | Diseño (previa a la Fase A) |
| [reporte-fase-a.md](reporte-fase-a.md) | Reporte de implementación con la evidencia de las 14 verificaciones (83 pruebas en ese momento) | Fase A |
| [revision-fase-a-codigo.md](revision-fase-a-codigo.md) | Volcado íntegro del código para revisión; los archivos que cambiaron después están en el lote 2 | Fase A, revisión |
| [revision-fase-a-pruebas-docs.md](revision-fase-a-pruebas-docs.md) | Volcado íntegro de pruebas y documentación para revisión; los archivos que cambiaron después están en el lote 2 | Fase A, revisión |
| [revision-fase-a-lote2.md](revision-fase-a-lote2.md) | Volcado de los 12 archivos que cambiaron en el lote de correcciones (98 pruebas) | Fase A, correcciones |
| [reporte-fase-b.md](reporte-fase-b.md) | Reporte de implementación de la estación simulada con la evidencia de las 9 verificaciones | Fase B |
| [revision-fase-b-codigo.md](revision-fase-b-codigo.md) | Volcado íntegro del código nuevo o modificado de la Fase B | Fase B, revisión |
| [revision-fase-b-pruebas-docs.md](revision-fase-b-pruebas-docs.md) | Volcado íntegro de pruebas, configuración y documentación nuevas o modificadas de la Fase B | Fase B, revisión |
| [revision-fase-b-lote2.md](revision-fase-b-lote2.md) | Volcado de los archivos que cambiaron en el lote de cierre: reanclaje tras un salto del reloj de pared (155 pruebas en station) | Fase B, correcciones |
```
