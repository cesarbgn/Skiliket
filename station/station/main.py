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
