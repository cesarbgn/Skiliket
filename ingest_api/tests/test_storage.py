import json
import threading
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from app.models import WeatherReading
from app.storage import DuplicateReadingError, JsonlStorage, StoredReading
from tests.conftest import FakeClock, make_payload

BASE = datetime.now(UTC).replace(microsecond=0) - timedelta(hours=1)
T0 = datetime(2026, 1, 10, tzinfo=UTC)
WEEK = timedelta(days=7)


def stored(offset_s: int = 0, station_id: str = "GDL-001") -> StoredReading:
    data = make_payload(station_id=station_id, timestamp=BASE + timedelta(seconds=offset_s))
    return StoredReading(
        reading_id=f"r-{offset_s}",
        received_at=datetime.now(UTC),
        reading=WeatherReading.model_validate_json(json.dumps(data)),
    )


def stored_at(ts: datetime, reading_id: str) -> StoredReading:
    """Lectura con timestamp arbitrario (la ventana se evalúa contra ese mismo instante)."""
    data = make_payload(timestamp=ts)
    reading = WeatherReading.model_validate_json(json.dumps(data), context={"now": ts})
    return StoredReading(reading_id=reading_id, received_at=ts, reading=reading)


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


class _HalfThenFail:
    """Envuelve el archivo: la primera escritura deja la mitad de los bytes y lanza OSError."""

    def __init__(self, real) -> None:
        self._real = real
        self.failed = False

    def write(self, data: bytes) -> int:
        if not self.failed:
            self.failed = True
            self._real.write(data[: len(data) // 2])
            self._real.flush()
            raise OSError("disco lleno (simulado)")
        return self._real.write(data)

    def __getattr__(self, name: str):
        return getattr(self._real, name)


def test_partial_write_failure_does_not_glue_next_reading(tmp_path: Path, log_records) -> None:
    path = tmp_path / "r.jsonl"
    storage = JsonlStorage(path)
    storage.save(stored(0))
    storage._file = _HalfThenFail(storage._file)
    with pytest.raises(OSError):
        storage.save(stored(1))
    storage.save(stored(2))
    storage.close()

    raw_lines = path.read_bytes().split(b"\n")
    assert raw_lines[-1] == b""
    assert json.loads(raw_lines[-2])["reading_id"] == "r-2"

    rebuilt = JsonlStorage(path)
    assert rebuilt.indexed_count == 2
    skipped = [r for r in log_records if getattr(r, "event", None) == "storage_line_skipped"]
    assert [r.line for r in skipped] == [2]
    rebuilt.save(stored(1))  # la lectura fallida nunca entró al índice
    with pytest.raises(DuplicateReadingError):
        rebuilt.save(stored(2))
    rebuilt.close()


def test_prune_drops_keys_outside_retention(tmp_path: Path) -> None:
    clock = FakeClock(T0)
    storage = JsonlStorage(tmp_path / "r.jsonl", max_past_age=WEEK, clock=clock)
    old_a = stored_at(T0 - timedelta(days=6), "old-a")
    old_b = stored_at(T0 - timedelta(days=6, hours=12), "old-b")
    recent = stored_at(T0 - timedelta(hours=1), "recent")
    for s in (old_a, old_b, recent):
        storage.save(s)

    # Retención = 7 días + 1 h; en T0 + 2 días el corte es T0 - 5 días - 1 h.
    clock.advance(days=2)
    before = storage.indexed_count
    storage.save(stored_at(clock.now - timedelta(minutes=1), "new"))
    after = storage.indexed_count
    assert (before, after) == (3, 2)

    with pytest.raises(DuplicateReadingError):
        storage.save(recent)
    storage.save(old_a)  # su clave se podó (la API ya la rechazaría con 422)
    storage.close()


def test_prune_runs_at_most_every_5_minutes(tmp_path: Path, monkeypatch) -> None:
    clock = FakeClock(T0)
    storage = JsonlStorage(tmp_path / "r.jsonl", max_past_age=WEEK, clock=clock)
    calls: list[datetime] = []
    original = storage._prune_locked

    def spy(now: datetime) -> None:
        calls.append(now)
        original(now)

    monkeypatch.setattr(storage, "_prune_locked", spy)
    for i in range(10):
        clock.advance(seconds=20)
        storage.save(stored_at(clock.now - timedelta(seconds=1), f"r{i}"))
    assert calls == []  # 200 s desde la creación
    clock.advance(minutes=2)
    storage.save(stored_at(clock.now - timedelta(seconds=1), "r10"))
    assert calls == [clock.now]
    storage.close()


def test_rebuild_loads_only_keys_inside_retention(tmp_path: Path) -> None:
    path = tmp_path / "r.jsonl"
    writer = JsonlStorage(path, max_past_age=timedelta(days=365), clock=FakeClock(T0))
    for i in range(200):
        writer.save(stored_at(T0 - timedelta(hours=i, minutes=30), f"r{i}"))
    writer.close()
    assert len(path.read_text(encoding="utf-8").splitlines()) == 200

    reader = JsonlStorage(path, max_past_age=timedelta(hours=100), clock=FakeClock(T0))
    # Retención = 100 h + 1 h: entran las de i h 30 min con i <= 100.
    assert reader.indexed_count == 101
    reader.close()


def test_record_format(tmp_path: Path) -> None:
    storage = JsonlStorage(tmp_path / "r.jsonl")
    storage.save(stored(0))
    storage.close()
    [record] = [json.loads(line) for line in (tmp_path / "r.jsonl").read_text().splitlines()]
    assert set(record) == {"reading_id", "received_at", "reading"}
    assert record["reading"]["timestamp"].endswith("Z")
    assert record["received_at"].endswith("Z")
