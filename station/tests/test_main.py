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
