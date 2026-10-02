# Revisión Fase B — pruebas y documentación

Volcado literal generado por script desde los bytes de cada archivo. Cada bloque está delimitado por una cerca de backticks más larga que cualquier secuencia de backticks del archivo. N = líneas según `wc -l` (saltos de línea); sha256 = hash de los bytes del archivo.

Pruebas nuevas (`station/tests/`, `contract_tests/`), ADR 0002, reporte de la fase y los dos README modificados (`README.md`, `docs/historial/README.md`).

=== station/tests/conftest.py === (162 líneas, sha256: b7feae7b08749e406e773ccd5b781cc30378b55a26c502011b48d3107c4b6b56)

```python
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
```

=== station/tests/test_config.py === (149 líneas, sha256: d24c110567896f9cd7af7c9e610977aa32928f08789cec9ec6b45880ad5df4ea)

```python
import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from conftest import KEY
from station.config import (
    DEFAULT_INGEST_URL,
    REPLAY_MAX_HOURS,
    ConfigError,
    Secret,
    default_seed,
    load_config,
)

STATION_DIR = Path(__file__).resolve().parents[1]
STATION_VARS = (
    "STATION_ID", "STATION_KEY", "INGEST_URL", "INTERVAL_S", "SEED", "LAT", "LON", "ELEVATION_M",
    "TZ_OFFSET_H", "PREVAILING_WIND_DEG", "REPLAY_HOURS", "REPLAY_DURATION_MIN", "FAULT_RATE",
    "BUFFER_MAX", "MAX_READINGS", "HTTP_CONNECT_TIMEOUT_S", "HTTP_READ_TIMEOUT_S",
)
VALID = {"STATION_ID": "GDL-001", "STATION_KEY": KEY}


def run_main(env_overrides: dict[str, str | None]) -> subprocess.CompletedProcess[str]:
    env = {k: v for k, v in os.environ.items() if k not in STATION_VARS}
    for name, value in {**VALID, **env_overrides}.items():
        if value is not None:
            env[name] = value
    return subprocess.run(
        [sys.executable, "-m", "station.main"],
        cwd=STATION_DIR,
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
    )


INVALID = [
    ("STATION_ID", None),
    ("STATION_ID", "gdl-001"),
    ("STATION_ID", "GDL-0001"),
    ("STATION_ID", "GDL-00a"),
    ("STATION_KEY", None),
    ("STATION_KEY", "short"),
    ("STATION_KEY", "con espacios dentro de la clave larga"),
    ("STATION_KEY", "ñ" * 20),
    ("INGEST_URL", "ftp://127.0.0.1/v1/readings"),
    ("INGEST_URL", "no-es-una-url"),
    ("INGEST_URL", "http://user:pass@127.0.0.1:8000/v1/readings"),
    ("INGEST_URL", "http://127.0.0.1:99999/v1/readings"),
    ("INTERVAL_S", "9"),
    ("INTERVAL_S", "3601"),
    ("INTERVAL_S", "60.0"),
    ("INTERVAL_S", "abc"),
    ("SEED", "-1"),
    ("SEED", "1.5"),
    ("LAT", "90.1"),
    ("LON", "-180.5"),
    ("ELEVATION_M", "5001"),
    ("TZ_OFFSET_H", "15"),
    ("TZ_OFFSET_H", "nan"),
    ("PREVAILING_WIND_DEG", "360"),
    ("REPLAY_HOURS", str(REPLAY_MAX_HOURS + 1)),
    ("REPLAY_HOURS", "-1"),
    ("REPLAY_DURATION_MIN", "-0.5"),
    ("FAULT_RATE", "1.01"),
    ("FAULT_RATE", "-0.1"),
    ("FAULT_RATE", "inf"),
    ("BUFFER_MAX", "0"),
    ("MAX_READINGS", "-1"),
    ("HTTP_CONNECT_TIMEOUT_S", "0"),
    ("HTTP_READ_TIMEOUT_S", "121"),
]


@pytest.mark.parametrize(("name", "value"), INVALID, ids=[f"{n}={v}" for n, v in INVALID])
def test_invalid_config_exits_2_without_printing_key(name: str, value: str | None) -> None:
    result = run_main({name: value})
    assert result.returncode == 2, result.stdout + result.stderr
    output = result.stdout + result.stderr
    assert KEY not in output
    [line] = [json.loads(x) for x in result.stdout.splitlines()]
    assert line["level"] == "ERROR" and line["event"] == "config_error"
    assert name in line["msg"]
    if value and len(value) >= 5:  # los valores cortos ("9") aparecen por azar en el ts del log
        # Ningún valor se refleja (por si alguien pega la clave en la variable equivocada).
        assert value not in output


def test_key_pasted_in_another_variable_is_not_echoed() -> None:
    result = run_main({"INTERVAL_S": KEY})
    assert result.returncode == 2 and KEY not in result.stdout + result.stderr


def env(**overrides: str) -> dict[str, str]:
    return {**VALID, **overrides}


def test_defaults() -> None:
    cfg = load_config(env())
    assert cfg.ingest_url == DEFAULT_INGEST_URL
    assert (cfg.interval_s, cfg.lat, cfg.lon, cfg.elevation_m, cfg.tz_offset_h) == (
        60, 20.67, -103.35, 1550.0, -6.0
    )
    assert (cfg.replay_hours, cfg.replay_duration_min, cfg.fault_rate) == (0, 2.0, 0.0)
    assert (cfg.buffer_max, cfg.max_readings) == (1000, 0)
    assert cfg.station_key.reveal() == KEY


def test_default_seed_is_sha256_of_station_id() -> None:
    expected = int.from_bytes(hashlib.sha256(b"GDL-002").digest()[:8], "big")
    assert default_seed("GDL-002") == expected
    assert load_config(env(STATION_ID="GDL-002")).seed == expected
    assert len({default_seed(f"GDL-00{i}") for i in (1, 2, 3)}) == 3


def test_explicit_values() -> None:
    url = "https://x.test/v1/readings"
    cfg = load_config(
        env(SEED="42", INTERVAL_S="10", FAULT_RATE="0.3", REPLAY_HOURS="2",
            REPLAY_DURATION_MIN="0", MAX_READINGS="6", INGEST_URL=url)
    )
    assert (cfg.seed, cfg.interval_s, cfg.fault_rate, cfg.replay_hours) == (42, 10, 0.3, 2)
    assert (cfg.replay_duration_min, cfg.max_readings, cfg.ingest_url) == (0.0, 6, url)


def test_empty_optional_values_use_defaults() -> None:
    cfg = load_config(env(SEED="", INTERVAL_S="  "))
    assert cfg.seed == default_seed("GDL-001") and cfg.interval_s == 60


def test_key_never_in_repr_or_errors() -> None:
    cfg = load_config(env())
    assert KEY not in repr(cfg) and KEY not in str(cfg)
    assert KEY not in repr(cfg.station_key) and KEY not in f"{cfg.station_key}"
    with pytest.raises(ConfigError) as info:
        load_config(env(INTERVAL_S="x"))
    assert KEY not in str(info.value)


def test_secret_equality_does_not_leak() -> None:
    assert Secret("a" * 20) == Secret("a" * 20)
    assert repr(Secret("a" * 20)) == "Secret('***')"
```

=== station/tests/test_weather_model.py === (331 líneas, sha256: 2e82a995cb1871fb25dbb614ccb7b519c252efc23d9c76b954c1542ba4037161)

```python
"""Propiedades del modelo sobre 24 h simuladas a 60 s, 10 s y 300 s.

Fechas: 2026-04-15 (época seca) y 2026-10-01 (fin de lluvias), desde la medianoche local
(06:00Z). 20 semillas por fecha e intervalo. Las series se guardan en columnas compactas
(array) y se reutilizan entre pruebas."""

import hashlib
import json
import math
import re
import statistics
from array import array
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from functools import lru_cache
from typing import Any

import pytest

from station import solar, weather_model
from station.config import default_seed
from station.faults import FaultInjector
from station.weather_model import ReadingGenerator, Site, WeatherModel, relative_humidity

SITE = Site()
DATES = ("2026-04-15", "2026-10-01")
INTERVALS = (60, 10, 300)
SEEDS = range(20)

# Rangos de validación del API (ingest_api/app/models.py).
API_RANGES = {
    "air_temperature_c": (-40, 60),
    "relative_humidity_pct": (0, 100),
    "station_pressure_hpa": (500, 1100),
    "wind_speed_ms": (0, 75),
    "wind_gust_ms": (0, 100),
    "wind_direction_deg": (0, 359),
    "precipitation_mm": (0, 200),
    "solar_radiation_wm2": (0, 1500),
}
_TENTHS = re.compile(r"^-?[0-9]+(\.[0-9])?$")
_HUNDREDTHS = re.compile(r"^[0-9]+(\.[0-9]{1,2})?$")
_TS = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$")


def start_of(date: str) -> datetime:
    return datetime.fromisoformat(f"{date}T06:00:00+00:00")  # 00:00 local (UTC-6)


@dataclass
class Series:
    hour: array = field(default_factory=lambda: array("d"))
    temp: array = field(default_factory=lambda: array("d"))
    rh: array = field(default_factory=lambda: array("d"))
    pressure: array = field(default_factory=lambda: array("d"))
    speed: array = field(default_factory=lambda: array("d"))
    gust: array = field(default_factory=lambda: array("d"))
    direction: array = field(default_factory=lambda: array("d"))  # -1 = null
    precip: array = field(default_factory=lambda: array("d"))
    radiation: array = field(default_factory=lambda: array("d"))
    battery: array = field(default_factory=lambda: array("d"))
    raining: array = field(default_factory=lambda: array("b"))
    elevation: array = field(default_factory=lambda: array("d"))
    clear_sky: array = field(default_factory=lambda: array("d"))
    violations: list[str] = field(default_factory=list)
    digest: str = ""

    @property
    def dry(self) -> bool:
        return not any(self.raining) and not any(self.precip)


def _check_payload(p: dict[str, Any], interval: int, expected_seq: int, out: list[str]) -> None:
    def bad(msg: str) -> None:
        if len(out) < 20:
            out.append(f"seq {p['sequence']}: {msg}")

    if type(p["interval_s"]) is not int or p["interval_s"] != interval:
        bad("interval_s no es int")
    if type(p["sequence"]) is not int or p["sequence"] != expected_seq:
        bad("sequence")
    if not _TS.fullmatch(p["timestamp"]):
        bad("timestamp")
    m = p["measurements"]
    for name, (lo, hi) in API_RANGES.items():
        value = m[name]
        if value is not None and not lo <= value <= hi:
            bad(f"{name}={value} fuera de [{lo}, {hi}]")
    for name in ("air_temperature_c", "station_pressure_hpa", "wind_speed_ms", "wind_gust_ms",
                 "precipitation_mm"):
        if type(m[name]) is not float or not _TENTHS.fullmatch(json.dumps(m[name])):
            bad(f"{name} serializa como {json.dumps(m[name])}")
    for name in ("relative_humidity_pct", "solar_radiation_wm2"):
        if type(m[name]) is not int:
            bad(f"{name} no es int")
    if m["wind_direction_deg"] is not None and type(m["wind_direction_deg"]) is not int:
        bad("wind_direction_deg no es int")
    tips = m["precipitation_mm"] * 5
    if abs(tips - round(tips)) > 1e-9:
        bad(f"precipitation_mm={m['precipitation_mm']} no es múltiplo de 0.2")
    battery = p["device"]["battery_v"]
    if not 12.0 <= battery <= 13.8 or not _HUNDREDTHS.fullmatch(json.dumps(battery)):
        bad(f"battery_v={battery}")
    if p["device"]["firmware"] != "sim-0.1.0" or "dew_point_c" in m:
        bad("device/dew_point")


@lru_cache(maxsize=None)
def simulate(date: str, interval: int, seed: int, station_id: str = "GDL-001") -> Series:
    generator = ReadingGenerator(station_id, interval, seed, SITE)
    s = Series()
    sha = hashlib.sha256()
    start = start_of(date)
    for k in range(86400 // interval):
        ts = start + timedelta(seconds=k * interval)
        p = generator.next(ts)
        sha.update(json.dumps(p).encode())
        _check_payload(p, interval, k, s.violations)
        m = p["measurements"]
        local = ts + timedelta(hours=SITE.tz_offset_h)
        s.hour.append(local.hour + local.minute / 60 + local.second / 3600)
        s.temp.append(m["air_temperature_c"])
        s.rh.append(m["relative_humidity_pct"])
        s.pressure.append(m["station_pressure_hpa"])
        s.speed.append(m["wind_speed_ms"])
        s.gust.append(m["wind_gust_ms"])
        s.direction.append(-1 if m["wind_direction_deg"] is None else m["wind_direction_deg"])
        s.precip.append(m["precipitation_mm"])
        s.radiation.append(m["solar_radiation_wm2"])
        s.battery.append(p["device"]["battery_v"])
        s.raining.append(generator.model.raining)
        elevation = solar.elevation_deg(ts, SITE.lat, SITE.lon)
        s.elevation.append(elevation)
        s.clear_sky.append(solar.clear_sky_ghi(elevation, SITE.elevation_m))
    s.digest = sha.hexdigest()
    return s


def all_series(interval: int):  # type: ignore[no-untyped-def]
    for date in DATES:
        for seed in SEEDS:
            yield date, seed, simulate(date, interval, seed)


@pytest.mark.parametrize("interval", INTERVALS)
def test_payload_values_within_api_ranges_and_formats(interval: int) -> None:
    for date, seed, s in all_series(interval):
        assert s.violations == [], (date, seed)


@pytest.mark.parametrize("interval", INTERVALS)
def test_humidity_in_range_and_high_when_raining(interval: int) -> None:
    rainy_steps = 0
    for date, seed, s in all_series(interval):
        assert all(0 <= v <= 100 for v in s.rh)
        for rh, precip, raining in zip(s.rh, s.precip, s.raining, strict=True):
            if precip > 0 or raining:
                rainy_steps += 1
                assert rh >= 90, (date, seed, rh)
    assert rainy_steps > 0, "ninguna serie tuvo lluvia: la prueba no comprobaría nada"


@pytest.mark.parametrize("interval", INTERVALS)
def test_gust_and_direction(interval: int) -> None:
    calm = 0
    for date, seed, s in all_series(interval):
        for speed, gust, direction in zip(s.speed, s.gust, s.direction, strict=True):
            assert gust >= speed
            if speed < 0.5:
                calm += 1
                assert direction == -1, (date, seed, speed, direction)
            else:
                assert 0 <= direction <= 359 and direction == int(direction)
    assert calm > 0, "no hubo calmas: la regla de null no se ejercitó"


@pytest.mark.parametrize("interval", INTERVALS)
def test_radiation_zero_at_night_and_bounded_by_clear_sky(interval: int) -> None:
    for date, seed, s in all_series(interval):
        for rad, elevation, clear in zip(s.radiation, s.elevation, s.clear_sky, strict=True):
            if elevation <= 0:
                assert rad == 0
            assert rad <= clear + 0.5


@pytest.mark.parametrize("date", DATES)
def test_clear_sky_maximum_between_900_and_1200(date: str) -> None:
    s = simulate(date, 60, 0)
    assert 900 <= max(s.clear_sky) <= 1200
    # Con nubes, la máxima medida del día es menor o igual; en algún día es casi despejado.
    assert max(max(simulate(date, 60, seed).radiation) for seed in SEEDS) >= 900


def _local_hour_of(s: Series, values: array, pick) -> float:  # type: ignore[no-untyped-def]
    index = pick(range(len(values)), key=values.__getitem__)
    return s.hour[index]


@pytest.mark.parametrize("interval", INTERVALS)
@pytest.mark.parametrize("date", DATES)
def test_tmax_and_tmin_hours_on_dry_days(date: str, interval: int) -> None:
    dry = [simulate(date, interval, seed) for seed in SEEDS if simulate(date, interval, seed).dry]
    assert len(dry) >= 10, f"solo {len(dry)} días secos de 20"
    tmax_ok = sum(13 <= _local_hour_of(s, s.temp, max) <= 17 for s in dry)
    tmin_ok = sum(5 <= _local_hour_of(s, s.temp, min) <= 8 for s in dry)
    assert tmax_ok >= 0.9 * len(dry), f"Tmax en 13-17 h: {tmax_ok}/{len(dry)}"
    assert tmin_ok >= 0.9 * len(dry), f"Tmin en 5-8 h: {tmin_ok}/{len(dry)}"


@pytest.mark.parametrize("interval", INTERVALS)
@pytest.mark.parametrize("date", DATES)
def test_temperature_humidity_anticorrelated_on_dry_days(date: str, interval: int) -> None:
    for seed in SEEDS:
        s = simulate(date, interval, seed)
        if s.dry:
            assert statistics.correlation(s.temp, s.rh) < -0.5, seed


def test_precipitation_has_no_float_artifacts() -> None:
    # El problema que se evita: 3 vueltas x 0.2 en flotante.
    assert json.dumps(3 * 0.2) == "0.6000000000000001"
    assert json.dumps(weather_model._round(3 * 0.2, 1)) == "0.6"
    assert json.dumps(weather_model._round(-0.04, 1)) == "0.0"  # sin "-0.0"
    for tips in range(0, 1001):
        assert _TENTHS.fullmatch(json.dumps(weather_model._round(tips * 0.2, 1)))
    seen = {v for _, _, s in all_series(60) for v in s.precip if v > 0}
    assert seen and all(_TENTHS.fullmatch(json.dumps(v)) for v in seen)


@pytest.mark.parametrize("interval", INTERVALS)
def test_same_seed_is_byte_identical(interval: int) -> None:
    # Serie en caché frente a una ejecución nueva con la misma semilla: mismos bytes.
    cached = simulate("2026-10-01", interval, 3).digest
    generator = ReadingGenerator("GDL-001", interval, 3, SITE)
    start = start_of("2026-10-01")
    sha = hashlib.sha256()
    for k in range(86400 // interval):
        sha.update(json.dumps(generator.next(start + timedelta(seconds=k * interval))).encode())
    assert sha.hexdigest() == cached


def test_different_seeds_and_stations_differ() -> None:
    digests = {simulate("2026-10-01", 60, seed).digest for seed in SEEDS}
    assert len(digests) == len(SEEDS)
    stations = ("GDL-001", "GDL-002", "GDL-003")
    by_station = {
        simulate("2026-10-01", 60, default_seed(sid), sid).digest for sid in stations
    }
    assert len(by_station) == 3


def test_fault_rate_does_not_change_unmutated_measurements() -> None:
    start = start_of("2026-10-01")
    results = {}
    for rate in (0.0, 0.3):
        generator = ReadingGenerator("GDL-001", 60, 99, SITE)
        injector = FaultInjector(rate, 99)
        results[rate] = []
        for k in range(1440):
            ts = start + timedelta(minutes=k)
            results[rate].append(injector.maybe_inject(generator.next(ts), ts))
    mutated = 0
    for (clean, _), (maybe, fault) in zip(results[0.0], results[0.3], strict=True):
        if fault is None:
            assert json.dumps(maybe) == json.dumps(clean)
        else:
            mutated += 1
    assert 300 <= mutated <= 570  # ~30 % de 1440


@pytest.mark.parametrize("interval", [10, 60, 300, 3600])
def test_precipitation_accumulates_per_interval(interval: int, monkeypatch: pytest.MonkeyPatch) -> None:
    # Lluvia constante de 7.3 mm/h durante 3 h: el total no depende del intervalo
    # (el balancín guarda el resto de menos de 0.2 mm entre intervalos).
    monkeypatch.setattr(weather_model, "_RAIN_MEAN_DURATION_S", 1e12)
    monkeypatch.setattr(weather_model, "_SD_RAIN_RATE", 0.0)
    model = WeatherModel(SITE, 1, interval)
    model._raining, model._rain_base_mmh, model._rain_z = True, 7.3, 0.0
    start = start_of("2026-10-01")
    total = 0.0
    per_step = []
    for k in range(1, 3 * 3600 // interval + 1):
        value = model.step(start + timedelta(seconds=k * interval))["precipitation_mm"]
        per_step.append(value)
        total += value
    assert 3 * 7.3 - 0.2 <= total <= 3 * 7.3 + 1e-9
    assert max(per_step) <= 7.3 * interval / 3600 + 0.2


@pytest.mark.parametrize("dt", [10, 60, 300, 3600])
def test_ar1_variance_and_correlation_do_not_depend_on_dt(dt: int) -> None:
    model = WeatherModel(SITE, 5, dt)
    tau, sd = 1800.0, 2.0
    x, values = 0.0, []
    for _ in range(40000):
        x = model._ar1(x, dt, tau, sd)
        values.append(x)
    assert statistics.pstdev(values) == pytest.approx(sd, rel=0.08)
    lag = max(1, round(1800 / dt))  # correlación a 30 min: exp(-1800/1800) = 0.37
    corr = statistics.correlation(values[:-lag], values[lag:])
    assert corr == pytest.approx(math.exp(-lag * dt / tau), abs=0.06)


def test_battery_charges_by_day_and_drains_at_night() -> None:
    s = simulate("2026-04-15", 60, 4)
    by_hour = {h: s.battery[int(h * 60)] for h in (6, 12, 17, 23)}
    assert by_hour[17] > by_hour[6]  # cargó durante el día
    assert by_hour[23] < by_hour[17]  # descarga de noche
    assert all(12.0 <= v <= 13.8 for v in s.battery)


def test_magnus_relative_humidity() -> None:
    assert relative_humidity(20.0, 20.0) == pytest.approx(100.0)
    assert relative_humidity(25.0, 15.0) == pytest.approx(53.9, abs=0.3)


def test_generator_sequence_and_timestamp() -> None:
    generator = ReadingGenerator("GDL-003", 60, 1, SITE)
    first = generator.next(datetime(2026, 10, 1, 21, 15, tzinfo=UTC))
    second = generator.next(datetime(2026, 10, 1, 21, 16, tzinfo=UTC))
    assert (first["sequence"], second["sequence"]) == (0, 1)
    assert first["timestamp"] == "2026-10-01T21:15:00Z" and first["station_id"] == "GDL-003"


def test_model_requires_advancing_aware_time() -> None:
    model = WeatherModel(SITE, 1, 60)
    with pytest.raises(ValueError):
        model.step(datetime(2026, 10, 1, 12, 0))
    model.step(datetime(2026, 10, 1, 12, 0, tzinfo=UTC))
    with pytest.raises(ValueError):
        model.step(datetime(2026, 10, 1, 12, 0, tzinfo=UTC))
```

=== station/tests/test_faults.py === (109 líneas, sha256: 944b6f7b4a7ceb59d4984f2779b82fa55756f37b749b0235cae022ba1f97a9bd)

```python
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
```

=== station/tests/test_sender.py === (275 líneas, sha256: 7edc951265442d4fd30ae59691c3ba10617abff1ca0a2fba04c737d6885f3f4f)

```python
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
```

=== station/tests/test_main.py === (189 líneas, sha256: dca795eb281e581e774c803d2260660c727e7b878fbce496227b55b1a226a42d)

```python
import json
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

=== contract_tests/pytest.ini === (6 líneas, sha256: 3851198d93711984502ced4106b62e8035afdd69a2fe8308a47cbd0626bc3eef)

```ini
[pytest]
# Contrato entre la estación (generador) y el API (modelo real). Se corre desde esta carpeta:
# importa el paquete "app" de ingest_api y el paquete "station" de station.
pythonpath = ../ingest_api ../station
testpaths = .
addopts = --basetemp=.pytest_tmp
```

=== contract_tests/test_contract.py === (96 líneas, sha256: 2a813c878fdfd8d093f8cf8db60ccf10a68544edc3c1f4165c16717966bb73cd)

```python
"""La salida del generador cumple el modelo REAL del API, y cada falla del catálogo
falla contra ese modelo con exactamente el (field, code) esperado.

24 h x 3 estaciones a 60 s = 4,320 payloads, serializados como los envía el emisor
(json.dumps compacto) y validados con el contexto "now" igual al timestamp de cada uno."""

import copy
import json
from datetime import UTC, datetime, timedelta
from functools import lru_cache
from typing import Any

import pytest
from pydantic import ValidationError

from app.errors import field_errors
from app.models import WeatherReading
from station.config import default_seed
from station.faults import CATALOG, Fault, FaultInjector
from station.weather_model import ReadingGenerator, Site

STATIONS = ("GDL-001", "GDL-002", "GDL-003")
INTERVAL_S = 60
START = datetime(2026, 10, 1, 6, 0, tzinfo=UTC)  # 00:00 hora local (UTC-6)


@lru_cache(maxsize=None)
def readings(station_id: str) -> tuple[tuple[datetime, dict[str, Any]], ...]:
    generator = ReadingGenerator(station_id, INTERVAL_S, default_seed(station_id), Site())
    out = []
    for k in range(86400 // INTERVAL_S):
        ts = START + timedelta(seconds=k * INTERVAL_S)
        out.append((ts, generator.next(ts)))
    return tuple(out)


def serialize(payload: dict[str, Any]) -> str:
    return json.dumps(payload, separators=(",", ":"))  # igual que station.sender.Sender


def validation_pairs(payload: dict[str, Any], now: datetime) -> set[tuple[str, str]]:
    """Pares (field, code) que devolvería el API en el 422; vacío si es válido."""
    try:
        WeatherReading.model_validate_json(serialize(payload), context={"now": now})
    except ValidationError as exc:
        return {(e["field"], e["code"]) for e in field_errors(exc.errors())}
    return set()


def test_all_4320_payloads_are_valid() -> None:
    total = valid = 0
    failures = []
    for station_id in STATIONS:
        for ts, payload in readings(station_id):
            total += 1
            pairs = validation_pairs(payload, ts)
            if pairs:
                failures.append((station_id, payload["sequence"], sorted(pairs)))
            else:
                valid += 1
    assert failures == []
    assert (total, valid) == (4320, 4320)


def test_stations_produce_different_series() -> None:
    temps = {
        s: tuple(p["measurements"]["air_temperature_c"] for _, p in readings(s)) for s in STATIONS
    }
    assert len(set(temps.values())) == 3


@pytest.mark.parametrize("fault", CATALOG, ids=lambda f: f.name)
def test_each_fault_fails_with_expected_field_and_code(fault: Fault) -> None:
    # Muestras a lo largo del día de las tres estaciones, incluidas lecturas en calma
    # (wind_direction_deg null), sobre las que gust_below_speed sube la velocidad a >= 1.
    checked = calm = 0
    for station_id in STATIONS:
        for ts, payload in readings(station_id)[::37]:
            mutated = copy.deepcopy(payload)
            fault.apply(mutated, ts)
            assert validation_pairs(mutated, ts) == {(fault.field, fault.code)}, payload["sequence"]
            checked += 1
            calm += payload["measurements"]["wind_direction_deg"] is None
    assert checked >= 100 and calm > 0


def test_injector_rate_1_over_all_payloads_matches_catalog() -> None:
    seen = set()
    for station_id in STATIONS:
        injector = FaultInjector(1.0, default_seed(station_id))
        for ts, payload in readings(station_id):
            mutated, fault = injector.maybe_inject(payload, ts)
            assert fault is not None
            assert validation_pairs(mutated, ts) == {(fault.field, fault.code)}
            seen.add(fault.name)
    assert seen == {f.name for f in CATALOG}
```

=== docs/adr/0002-estacion-simulada.md === (159 líneas, sha256: cc06ab2f9c11ae0e24f4c5dbccb650ad7d700f1992b9b91212e3a1d135c044fb)

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

- **Pruebas:** station 151, ingest_api 98, contract_tests 12. El contrato valida 4,320 payloads (24 h × 3 estaciones) contra el `WeatherReading` real, y cada falla devuelve exactamente su (field, code).
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

=== README.md === (117 líneas, sha256: f1b910fe70cdce0a2868368dbce9fb4ab5b35c2622d0825c6c8cc267c3cbddd6)

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

- **Detener:** Ctrl+C. La estación intenta vaciar el búfer durante 10 s como máximo e imprime un resumen JSON (`event: summary`).
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

`station/tests/test_weather_model.py` simula 24 h con 20 semillas, 2 fechas e intervalos de 60, 10 y 300 s: tarda varios minutos.

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

=== docs/historial/README.md === (14 líneas, sha256: e7247fba4def03b17606076d7bdbd70f27be10969030bb9295b2413fcc06eba7)

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
```

=== docs/historial/reporte-fase-b.md === (535 líneas, sha256: 98376eca7ca085a1b6f1f177698a5b6f2f3ae13f917795d85f5fab6106627c99)

````markdown
# Reporte Fase B — estación meteorológica simulada

**Fecha:** 2026-10-01
**Alcance:** generador, inyección de fallas y emisor de `station/`, más la suite de contrato `contract_tests/`. Sin Docker, sin diagramas y sin cambios en `ingest_api/app`.
**Entorno:** Windows 11, Python 3.13.5 (venv local `.venv`), uvicorn 0.54.0 real, procesos reales de estación.
**Decisiones:** [ADR 0002](../adr/0002-estacion-simulada.md).

## Resumen

| # | Verificación | Resultado |
|---|---|---|
| 1 | Tres suites (station, ingest_api, contract_tests) | 151 + 98 + 12 = 261 pasan; salidas completas al final |
| 2 | 3 estaciones en vivo, `INTERVAL_S=10`, `MAX_READINGS=6` | 18 líneas, 18 pares únicos, códigos de salida 0 |
| 3 | Réplica de 2 h y repetición | 120 líneas por estación; la repetición da 120 × 409; tras cambiar el minuto, 1 nuevo = 1 línea; 363 líneas = 363 pares |
| 4 | Fallas, `FAULT_RATE=0.3`, 60 lecturas × 3 estaciones | 55 inyectadas, 55 `fault_confirmed`, 0 `fault_mismatch`; los huecos de `sequence` = fallas |
| 5 | Caída del API de 50 s con `INTERVAL_S=10` | 6 reintentos; 18 de 18 en orden, sin pérdidas ni duplicados |
| 6 | Clave incorrecta / `station_id` ajeno | 401 y 403 con ERROR, 0 reintentos |
| 7 | Cierre ordenado | `CTRL_BREAK_EVENT` → resumen, `reason: signal`, código 0. SIGINT real no demostrado (ver punto 7) |
| 8 | `grep -F` de las 3 claves | 0 coincidencias (control positivo: 3 en el archivo de claves) |
| 9 | Payload real | Sin artefactos de flotante; enteros como enteros |

**Bug hallado durante la verificación y corregido:**
- **Síntoma:** en la primera corrida del punto 9 (réplica de 24 h con `REPLAY_DURATION_MIN=0`), la estación envió 549 de 1,440 lecturas, a partir de `sequence` 440.
- **Causa:** el bucle generaba todas las lecturas vencidas antes de enviar, así que desbordó `BUFFER_MAX=1000` y descartó las 440 más viejas. Además, `MAX_READINGS` cortaba al generar la última, y el vaciado de 10 s no alcanzó para el resto.
- **Corrección:** contrapresión en la réplica (la lectura siguiente se genera con el búfer vacío), y `MAX_READINGS` termina al entregar todo. Se añadieron 3 pruebas de regresión.
- **Repetición:** con el código corregido se repitieron los puntos 2 a 7 y 9. Los resultados de abajo son de esa corrida.

## 2. Tres estaciones en vivo

Claves generadas con `scripts/gen_station_key.py` (una por estación); el API recibió solo los hashes. JSONL: `data/verify-b/live.jsonl`.

```
estaciones terminaron en 42.5 s; exit codes: [0, 0, 0]
JSONL data\verify-b\live.jsonl: 18 líneas, 18 pares (station_id, timestamp) únicos
  por estación: {'GDL-001': 6, 'GDL-002': 6, 'GDL-003': 6}
  GDL-001: sequences [0, 1, 2, 3, 4, 5]; timestamps 2026-10-02T01:48:50Z .. 2026-10-02T01:49:40Z
  GDL-002: sequences [0, 1, 2, 3, 4, 5]; timestamps 2026-10-02T01:48:50Z .. 2026-10-02T01:49:40Z
  GDL-003: sequences [0, 1, 2, 3, 4, 5]; timestamps 2026-10-02T01:48:50Z .. 2026-10-02T01:49:40Z
{"ts": "2026-10-02T01:49:40.029086Z", "level": "INFO", "logger": "skilliket.station", "msg": "summary", "event": "summary", "station_id": "GDL-001", "reason": "max_readings", "summary": {"generated": 6, "created": 6, "duplicates": 0, "dropped_by_status": {}, "dropped_overflow": 0, "retries": 0, "faults_injected": 0, "faults_confirmed": 0, "faults_unexpected": 0, "unsent_at_exit": 0}}
{"ts": "2026-10-02T01:49:40.020331Z", "level": "INFO", "logger": "skilliket.station", "msg": "summary", "event": "summary", "station_id": "GDL-002", "reason": "max_readings", "summary": {"generated": 6, "created": 6, "duplicates": 0, "dropped_by_status": {}, "dropped_overflow": 0, "retries": 0, "faults_injected": 0, "faults_confirmed": 0, "faults_unexpected": 0, "unsent_at_exit": 0}}
{"ts": "2026-10-02T01:49:40.017595Z", "level": "INFO", "logger": "skilliket.station", "msg": "summary", "event": "summary", "station_id": "GDL-003", "reason": "max_readings", "summary": {"generated": 6, "created": 6, "duplicates": 0, "dropped_by_status": {}, "dropped_overflow": 0, "retries": 0, "faults_injected": 0, "faults_confirmed": 0, "faults_unexpected": 0, "unsent_at_exit": 0}}
```

## 3. Réplica y repetición

`INTERVAL_S=60`, `REPLAY_HOURS=2`, `REPLAY_DURATION_MIN=0`, `MAX_READINGS=120` (exactamente la réplica). Las corridas 1 y 2 son consecutivas. La 3 espera a que cambie el minuto: así hay un timestamp nuevo y se comprueba que líneas nuevas = timestamps nuevos con un valor distinto de cero.

```
--- corrida 1: 2.2 s, exit codes [0, 0, 0]
  GDL-001: lecturas esperadas 120, enviadas 120, líneas nuevas 120 (esperadas 120)
    resumen: created=120 duplicates=0 dropped={} generated=120
  GDL-002: lecturas esperadas 120, enviadas 120, líneas nuevas 120 (esperadas 120)
    resumen: created=120 duplicates=0 dropped={} generated=120
  GDL-003: lecturas esperadas 120, enviadas 120, líneas nuevas 120 (esperadas 120)
    resumen: created=120 duplicates=0 dropped={} generated=120
--- corrida 2: 2.0 s, exit codes [0, 0, 0]
  GDL-001: timestamps solapados con corridas previas = 120, nuevos = 0, líneas nuevas = 0
    resumen: created=0 duplicates=120 dropped={} generated=120
  GDL-002: timestamps solapados con corridas previas = 120, nuevos = 0, líneas nuevas = 0
    resumen: created=0 duplicates=120 dropped={} generated=120
  GDL-003: timestamps solapados con corridas previas = 120, nuevos = 0, líneas nuevas = 0
    resumen: created=0 duplicates=120 dropped={} generated=120
--- corrida 3: 2.2 s, exit codes [0, 0, 0]
  GDL-001: timestamps solapados con corridas previas = 119, nuevos = 1, líneas nuevas = 1
    resumen: created=1 duplicates=119 dropped={} generated=120
  GDL-002: timestamps solapados con corridas previas = 119, nuevos = 1, líneas nuevas = 1
    resumen: created=1 duplicates=119 dropped={} generated=120
  GDL-003: timestamps solapados con corridas previas = 119, nuevos = 1, líneas nuevas = 1
    resumen: created=1 duplicates=119 dropped={} generated=120
JSONL data\verify-b\replay.jsonl: 363 líneas, 363 pares (station_id, timestamp) únicos
  por estación: {'GDL-001': 121, 'GDL-002': 121, 'GDL-003': 121}
```

## 4. Fallas

`FAULT_RATE=0.3`, 60 lecturas por estación (réplica de 1 h a 60 s, lo más rápido posible). Las fallas `future_timestamp` dieron 422 aun en réplica, porque usan reloj actual + 10 min (ver ADR 0002, decisión 5).

```
JSONL data\verify-b\faults.jsonl: 125 líneas, 125 pares (station_id, timestamp) únicos
  por estación: {'GDL-001': 42, 'GDL-002': 39, 'GDL-003': 44}
  GDL-001: inyectadas=18 confirmed=18 mismatch=0 líneas=42 válidas esperadas=42
    huecos de sequence en JSONL: [0, 3, 6, 12, 13, 18, 21, 27, 28, 32, 35, 37, 38, 47, 49, 53, 54, 58]
    sequences con falla:        [0, 3, 6, 12, 13, 18, 21, 27, 28, 32, 35, 37, 38, 47, 49, 53, 54, 58]  -> coinciden: True
    resumen: {"generated": 60, "created": 42, "duplicates": 0, "dropped_by_status": {"422": 18}, "dropped_overflow": 0, "retries": 0, "faults_injected": 18, "faults_confirmed": 18, "faults_unexpected": 0, "unsent_at_exit": 0}
  GDL-002: inyectadas=21 confirmed=21 mismatch=0 líneas=39 válidas esperadas=39
    huecos de sequence en JSONL: [3, 5, 6, 8, 18, 23, 25, 29, 31, 34, 39, 41, 42, 48, 51, 52, 53, 55, 56, 57, 58]
    sequences con falla:        [3, 5, 6, 8, 18, 23, 25, 29, 31, 34, 39, 41, 42, 48, 51, 52, 53, 55, 56, 57, 58]  -> coinciden: True
    resumen: {"generated": 60, "created": 39, "duplicates": 0, "dropped_by_status": {"422": 21}, "dropped_overflow": 0, "retries": 0, "faults_injected": 21, "faults_confirmed": 21, "faults_unexpected": 0, "unsent_at_exit": 0}
  GDL-003: inyectadas=16 confirmed=16 mismatch=0 líneas=44 válidas esperadas=44
    huecos de sequence en JSONL: [6, 8, 12, 13, 15, 18, 21, 29, 31, 36, 40, 43, 44, 48, 49, 57]
    sequences con falla:        [6, 8, 12, 13, 15, 18, 21, 29, 31, 36, 40, 43, 44, 48, 49, 57]  -> coinciden: True
    resumen: {"generated": 60, "created": 44, "duplicates": 0, "dropped_by_status": {"422": 16}, "dropped_overflow": 0, "retries": 0, "faults_injected": 16, "faults_confirmed": 16, "faults_unexpected": 0, "unsent_at_exit": 0}
tipos de falla inyectados: {'bad_schema_version': 8, 'extra_field': 3, 'float_in_integer': 6, 'future_timestamp': 4, 'gust_below_speed': 4, 'humidity_over_100': 14, 'missing_required': 8, 'string_in_number': 3, 'temperature_below_min': 5}
```

Ejemplo de una línea `fault_confirmed` (lo esperado frente a lo recibido del API real):

```
{"ts": "2026-10-02T01:42:11.701783Z", "level": "INFO", "logger": "skilliket.station", "msg": "fault_confirmed", "event": "fault_check", "station_id": "GDL-001", "sequence": 0, "fault": "humidity_over_100", "expected": {"status": 422, "errors": [["measurements.relative_humidity_pct", "less_than_equal"]]}, "received": {"status": 422, "errors": [["measurements.relative_humidity_pct", "less_than_equal"]]}}
```

## 5. Caída del API

GDL-001 con `INTERVAL_S=10` y `MAX_READINGS=18`. El API se mata con `TerminateProcess` a los 35 s, queda caído 50 s (5 intervalos) y se reinicia con el mismo `STORAGE_PATH`.

```
[19:50:08] API arriba (pid 25532), STORAGE_PATH=data\verify-b\outage.jsonl
[19:50:43] API detenido (pid 25532, kill)
API caído 50.0 s (>= 4 intervalos de 10 s)
[19:51:35] API arriba (pid 15028), STORAGE_PATH=data\verify-b\outage.jsonl
estación terminó a los 161.3 s, exit code 0
send_retry: 6; buffered máx: 4
   retry seq=5 attempt=1 error=ConnectError delay_s=0.94 buffered=1
   retry seq=5 attempt=2 error=ConnectError delay_s=1.6 buffered=1
   retry seq=5 attempt=3 error=ConnectError delay_s=2.69 buffered=1
   retry seq=5 attempt=4 error=ConnectError delay_s=7.43 buffered=2
   retry seq=5 attempt=5 error=ConnectError delay_s=8.06 buffered=3
   retry seq=5 attempt=6 error=ConnectError delay_s=26.68 buffered=4
entregas (sequence, intento, status): [(0, 1, 201), (1, 1, 201), (2, 1, 201), (3, 1, 201), (4, 1, 201), (5, 7, 201), (6, 1, 201), (7, 1, 201), (8, 1, 201), (9, 1, 201), (10, 1, 201), (11, 1, 201), (12, 1, 201), (13, 1, 201), (14, 1, 201), (15, 1, 201), (16, 1, 201), (17, 1, 201)]
JSONL data\verify-b\outage.jsonl: 18 líneas, 18 pares (station_id, timestamp) únicos
  por estación: {'GDL-001': 18}
sequences en el JSONL, en orden de archivo: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17]
orden ascendente: True | sin pérdidas: True | sin duplicados: True
{"ts": "2026-10-02T01:52:50.025300Z", "level": "INFO", "logger": "skilliket.station", "msg": "summary", "event": "summary", "station_id": "GDL-001", "reason": "max_readings", "summary": {"generated": 18, "created": 18, "duplicates": 0, "dropped_by_status": {}, "dropped_overflow": 0, "retries": 6, "faults_injected": 0, "faults_confirmed": 0, "faults_unexpected": 0, "unsent_at_exit": 0}}
```

`buffered` se registra en cada reintento. Durante el último backoff (26.7 s) se acumularon más lecturas. Al volver el API, la 5 entró en su intento 7 y las siguientes salieron en orden en su primer intento.

## 6. Autenticación

```
--- auth401 (GDL-001 con una clave aleatoria no registrada), exit 0
   {"ts": "2026-10-02T01:52:53.057888Z", "level": "ERROR", "logger": "skilliket.station", "msg": "reading_rejected: 401, el API no reconoce STATION_KEY; revisa la clave (y que su hash esté en STATION_KEY_HASHES del API). No se reintenta.", "event": "reading", "station_id": "GDL-001", "sequence": 0, "timestamp": "2026-10-02T01:52:50Z", "status": 401, "attempt": 1, "outcome": "rejected", "reason": "check_station_key", "problem_type": "urn:skilliket:problem:unauthorized", "errors": []}
   {"ts": "2026-10-02T01:53:00.010205Z", "level": "ERROR", "logger": "skilliket.station", "msg": "reading_rejected: 401, el API no reconoce STATION_KEY; revisa la clave (y que su hash esté en STATION_KEY_HASHES del API). No se reintenta.", "event": "reading", "station_id": "GDL-001", "sequence": 1, "timestamp": "2026-10-02T01:53:00Z", "status": 401, "attempt": 1, "outcome": "rejected", "reason": "check_station_key", "problem_type": "urn:skilliket:problem:unauthorized", "errors": []}
   send_retry: 0
   {"summary": {"generated": 2, "created": 0, "duplicates": 0, "dropped_by_status": {"401": 2}, "dropped_overflow": 0, "retries": 0, ...}}
--- auth403 (station_id GDL-002 con la clave de GDL-001), exit 0
   {"ts": "2026-10-02T01:52:53.032961Z", "level": "ERROR", "logger": "skilliket.station", "msg": "reading_rejected: 403, la clave pertenece a otra estación; revisa STATION_ID y STATION_KEY. No se reintenta.", "event": "reading", "station_id": "GDL-002", "sequence": 0, "timestamp": "2026-10-02T01:52:50Z", "status": 403, "attempt": 1, "outcome": "rejected", "reason": "check_station_id", "problem_type": "urn:skilliket:problem:forbidden", "errors": []}
   {"ts": "2026-10-02T01:53:00.011762Z", "level": "ERROR", "logger": "skilliket.station", "msg": "reading_rejected: 403, la clave pertenece a otra estación; revisa STATION_ID y STATION_KEY. No se reintenta.", "event": "reading", "station_id": "GDL-002", "sequence": 1, "timestamp": "2026-10-02T01:53:00Z", "status": 403, "attempt": 1, "outcome": "rejected", "reason": "check_station_id", "problem_type": "urn:skilliket:problem:forbidden", "errors": []}
   send_retry: 0
   {"summary": {"generated": 2, "created": 0, "duplicates": 0, "dropped_by_status": {"403": 2}, "dropped_overflow": 0, "retries": 0, ...}}
JSONL data\verify-b\auth.jsonl: 0 líneas
```

Una solicitud por lectura (`attempt: 1`) y ningún `send_retry`.

## 7. Cierre ordenado

GDL-003 con `INTERVAL_S=10`, sin `MAX_READINGS`, lanzada con `CREATE_NEW_PROCESS_GROUP`. A los 25 s se le envía `CTRL_BREAK_EVENT` (llega como SIGBREAK).

```
terminó 0.37 s después de CTRL_BREAK_EVENT, exit code 0
{"ts": "2026-10-02T01:53:27.114672Z", "level": "INFO", "logger": "skilliket.station", "msg": "summary", "event": "summary", "station_id": "GDL-003", "reason": "signal", "summary": {"generated": 3, "created": 3, "duplicates": 0, "dropped_by_status": {}, "dropped_overflow": 0, "retries": 0, "faults_injected": 0, "faults_confirmed": 0, "faults_unexpected": 0, "unsent_at_exit": 0}}
JSONL data\verify-b\sigint.jsonl: 3 líneas, 3 pares (station_id, timestamp) únicos
```

**Lo que no se demostró con un proceso real:**
- **SIGINT (`CTRL_C_EVENT`):** en Windows llega a todo el grupo de consola, incluida la terminal que lo envía. Solo se probó el manejador dentro del mismo proceso, con `signal.raise_signal(SIGINT)` (`test_install_signal_handlers_sets_event`).
- **SIGTERM:** en Windows, `Popen.terminate()` es `TerminateProcess` y no entrega ninguna señal.
- El manejador es el mismo para las tres señales.

## 8. Búsqueda de las claves en claro

Patrones: las 3 claves generadas para la verificación. Alcance:
- 27 logs de procesos (6,983 líneas), con los resúmenes;
- la salida de la verificación;
- los 7 JSONL;
- los 3,287 archivos del proyecto, incluidos `.venv/` y `.git/`.

El archivo de claves estaba fuera del proyecto y se usó como control positivo.

```
--- coincidencias:
0
exit grep: 1 (1 = ninguna coincidencia)
--- control positivo: grep -oF en el archivo de claves -> 3
```

## 9. Payload real (bytes enviados al API)

GDL-002 con `SEED=4` (elegida porque llueve en la ventana), réplica de 24 h a 60 s. Los cuerpos se capturaron con un hook de `httpx` sobre el cliente real de la estación.

```
cuerpos enviados: 1440
con precipitation_mm > 0: 122
BODY {"schema_version":"1.0","station_id":"GDL-002","timestamp":"2026-10-01T22:16:00Z","sequence":1227,"interval_s":60,"measurements":{"air_temperature_c":25.7,"relative_humidity_pct":94,"station_pressure_hpa":841.5,"wind_speed_ms":4.5,"wind_gust_ms":8.3,"wind_direction_deg":235,"precipitation_mm":1.0,"solar_radiation_wm2":99},"device":{"battery_v":13.61,"firmware":"sim-0.1.0"}}
BODY {"schema_version":"1.0","station_id":"GDL-002","timestamp":"2026-10-01T22:17:00Z","sequence":1228,"interval_s":60,"measurements":{"air_temperature_c":25.5,"relative_humidity_pct":94,"station_pressure_hpa":841.4,"wind_speed_ms":3.9,"wind_gust_ms":6.3,"wind_direction_deg":229,"precipitation_mm":0.8,"solar_radiation_wm2":99},"device":{"battery_v":13.6,"firmware":"sim-0.1.0"}}
BODY {"schema_version":"1.0","station_id":"GDL-002","timestamp":"2026-10-01T22:18:00Z","sequence":1229,"interval_s":60,"measurements":{"air_temperature_c":25.3,"relative_humidity_pct":94,"station_pressure_hpa":841.5,"wind_speed_ms":5.7,"wind_gust_ms":8.2,"wind_direction_deg":228,"precipitation_mm":0.8,"solar_radiation_wm2":98},"device":{"battery_v":13.6,"firmware":"sim-0.1.0"}}
BODY {"schema_version":"1.0","station_id":"GDL-002","timestamp":"2026-10-01T01:49:00Z","sequence":0,"interval_s":60,"measurements":{"air_temperature_c":21.9,"relative_humidity_pct":62,"station_pressure_hpa":842.3,"wind_speed_ms":7.8,"wind_gust_ms":12.4,"wind_direction_deg":214,"precipitation_mm":0.0,"solar_radiation_wm2":0},"device":{"battery_v":12.8,"firmware":"sim-0.1.0"}}
valores de precipitation_mm serializados: {'0.0': 1318, '0.2': 25, '0.4': 21, '0.6': 12, '0.8': 28, '1.0': 36}
líneas en JSONL: 1440
JSONL de esa lectura: {"schema_version":"1.0","station_id":"GDL-002","timestamp":"2026-10-01T22:16:00Z","sequence":1227,"interval_s":60,"measurements":{"air_temperature_c":25.7,"relative_humidity_pct":94.0,"station_pressure_hpa":841.5,"wind_speed_ms":4.5,"wind_gust_ms":8.3,"wind_direction_deg":235,"precipitation_mm":1.0,"solar_radiation_wm2":99.0},"device":{"battery_v":13.61,"firmware":"sim-0.1.0"}}
```

**Lectura de la evidencia:**
- **Precipitación:** de las 1,440 lecturas, `precipitation_mm` solo toma 0.0, 0.2, 0.4, 0.6, 0.8 y 1.0. Nunca aparece `0.6000000000000001`.
- **Enteros:** `interval_s`, `sequence` y `wind_direction_deg` salen como enteros, sin `.0`.
- **Observación sobre el API (no se modificó):** en el JSONL, el API escribe `relative_humidity_pct` y `solar_radiation_wm2` como `94.0` y `99.0`, porque su modelo los declara `float` y normaliza al guardar. La estación los envía como enteros (`94`, `99`); ambos son válidos.

## Hallazgos y desviaciones

1. **Bug del bucle principal**, corregido (ver Resumen).
2. **`future_timestamp`** usa reloj actual + 10 min, no timestamp de la lectura + 10 min. Con la segunda opción, la falla no daría 422 en réplica ni con intervalos largos.
3. **`float_in_integer`** usa `float(INTERVAL_S)`, que es `60.0` con el intervalo por defecto.
4. **`test_main.py`** se añadió a la lista acordada de archivos de prueba.
5. **`PREVAILING_WIND_DEG`** (defecto 250) se añadió a las variables, porque la tabla aprobada pedía una dirección predominante configurable.
6. **Inercia térmica y curva diurna:** en la primera medición, Tmax caía después de las 17 h en ~10 % de los días secos. Se añadió inercia térmica (paso bajo de nubes y anomalía) y una bajada vespertina con pendiente desde el pico. Después: Tmax entre 13.6 y 15.4 h y Tmin en 5–8 h en ≥ 92 % de los días secos (abril y octubre).
7. **No se necesitó ningún cambio en el contrato** ni en `ingest_api/app`.
8. **Máquina lenta:** durante la sesión, el CPU estuvo limitado por el sistema (un bucle de referencia de 1 M de `sin` tardó entre 0.38 y 5.9 s), así que una corrida de la suite de `station` tardó 11 min 35 s. La corrida final, ya sin esa limitación, tardó 67 s.

## Salidas completas de las suites

### station

Comando: `cd station; ..\.venv\Scripts\python.exe -m pytest -v --durations=5` (la última línea, `exit N`, es el código de salida que agregó el script).

```
============================= test session starts =============================
platform win32 -- Python 3.13.5, pytest-9.1.1, pluggy-1.6.0 -- C:\Users\orizo\OneDrive\Documentos\TEC\SKILLIKET\.venv\Scripts\python.exe
cachedir: .pytest_cache
rootdir: C:\Users\orizo\OneDrive\Documentos\TEC\SKILLIKET\station
configfile: pytest.ini
testpaths: tests
plugins: anyio-4.15.1
collecting ... collected 151 items

tests/test_config.py::test_invalid_config_exits_2_without_printing_key[STATION_ID=None] PASSED [  0%]
tests/test_config.py::test_invalid_config_exits_2_without_printing_key[STATION_ID=gdl-001] PASSED [  1%]
tests/test_config.py::test_invalid_config_exits_2_without_printing_key[STATION_ID=GDL-0001] PASSED [  1%]
tests/test_config.py::test_invalid_config_exits_2_without_printing_key[STATION_ID=GDL-00a] PASSED [  2%]
tests/test_config.py::test_invalid_config_exits_2_without_printing_key[STATION_KEY=None] PASSED [  3%]
tests/test_config.py::test_invalid_config_exits_2_without_printing_key[STATION_KEY=short] PASSED [  3%]
tests/test_config.py::test_invalid_config_exits_2_without_printing_key[STATION_KEY=con espacios dentro de la clave larga] PASSED [  4%]
tests/test_config.py::test_invalid_config_exits_2_without_printing_key[STATION_KEY=\xf1\xf1\xf1\xf1\xf1\xf1\xf1\xf1\xf1\xf1\xf1\xf1\xf1\xf1\xf1\xf1\xf1\xf1\xf1\xf1] PASSED [  5%]
tests/test_config.py::test_invalid_config_exits_2_without_printing_key[INGEST_URL=ftp://127.0.0.1/v1/readings] PASSED [  5%]
tests/test_config.py::test_invalid_config_exits_2_without_printing_key[INGEST_URL=no-es-una-url] PASSED [  6%]
tests/test_config.py::test_invalid_config_exits_2_without_printing_key[INGEST_URL=http://user:pass@127.0.0.1:8000/v1/readings] PASSED [  7%]
tests/test_config.py::test_invalid_config_exits_2_without_printing_key[INGEST_URL=http://127.0.0.1:99999/v1/readings] PASSED [  7%]
tests/test_config.py::test_invalid_config_exits_2_without_printing_key[INTERVAL_S=9] PASSED [  8%]
tests/test_config.py::test_invalid_config_exits_2_without_printing_key[INTERVAL_S=3601] PASSED [  9%]
tests/test_config.py::test_invalid_config_exits_2_without_printing_key[INTERVAL_S=60.0] PASSED [  9%]
tests/test_config.py::test_invalid_config_exits_2_without_printing_key[INTERVAL_S=abc] PASSED [ 10%]
tests/test_config.py::test_invalid_config_exits_2_without_printing_key[SEED=-1] PASSED [ 11%]
tests/test_config.py::test_invalid_config_exits_2_without_printing_key[SEED=1.5] PASSED [ 11%]
tests/test_config.py::test_invalid_config_exits_2_without_printing_key[LAT=90.1] PASSED [ 12%]
tests/test_config.py::test_invalid_config_exits_2_without_printing_key[LON=-180.5] PASSED [ 13%]
tests/test_config.py::test_invalid_config_exits_2_without_printing_key[ELEVATION_M=5001] PASSED [ 13%]
tests/test_config.py::test_invalid_config_exits_2_without_printing_key[TZ_OFFSET_H=15] PASSED [ 14%]
tests/test_config.py::test_invalid_config_exits_2_without_printing_key[TZ_OFFSET_H=nan] PASSED [ 15%]
tests/test_config.py::test_invalid_config_exits_2_without_printing_key[PREVAILING_WIND_DEG=360] PASSED [ 15%]
tests/test_config.py::test_invalid_config_exits_2_without_printing_key[REPLAY_HOURS=168] PASSED [ 16%]
tests/test_config.py::test_invalid_config_exits_2_without_printing_key[REPLAY_HOURS=-1] PASSED [ 17%]
tests/test_config.py::test_invalid_config_exits_2_without_printing_key[REPLAY_DURATION_MIN=-0.5] PASSED [ 17%]
tests/test_config.py::test_invalid_config_exits_2_without_printing_key[FAULT_RATE=1.01] PASSED [ 18%]
tests/test_config.py::test_invalid_config_exits_2_without_printing_key[FAULT_RATE=-0.1] PASSED [ 19%]
tests/test_config.py::test_invalid_config_exits_2_without_printing_key[FAULT_RATE=inf] PASSED [ 19%]
tests/test_config.py::test_invalid_config_exits_2_without_printing_key[BUFFER_MAX=0] PASSED [ 20%]
tests/test_config.py::test_invalid_config_exits_2_without_printing_key[MAX_READINGS=-1] PASSED [ 21%]
tests/test_config.py::test_invalid_config_exits_2_without_printing_key[HTTP_CONNECT_TIMEOUT_S=0] PASSED [ 21%]
tests/test_config.py::test_invalid_config_exits_2_without_printing_key[HTTP_READ_TIMEOUT_S=121] PASSED [ 22%]
tests/test_config.py::test_key_pasted_in_another_variable_is_not_echoed PASSED [ 23%]
tests/test_config.py::test_defaults PASSED                               [ 23%]
tests/test_config.py::test_default_seed_is_sha256_of_station_id PASSED   [ 24%]
tests/test_config.py::test_explicit_values PASSED                        [ 25%]
tests/test_config.py::test_empty_optional_values_use_defaults PASSED     [ 25%]
tests/test_config.py::test_key_never_in_repr_or_errors PASSED            [ 26%]
tests/test_config.py::test_secret_equality_does_not_leak PASSED          [ 27%]
tests/test_faults.py::test_catalog_has_the_nine_agreed_faults PASSED     [ 27%]
tests/test_faults.py::test_rate_1_mutates_every_reading_and_covers_every_fault PASSED [ 28%]
tests/test_faults.py::test_rate_0_never_mutates PASSED                   [ 29%]
tests/test_faults.py::test_rate_0_3_proportion_over_2000 PASSED          [ 29%]
tests/test_faults.py::test_mutation_does_not_touch_original PASSED       [ 30%]
tests/test_faults.py::test_each_mutation[humidity_over_100] PASSED       [ 31%]
tests/test_faults.py::test_each_mutation[temperature_below_min] PASSED   [ 31%]
tests/test_faults.py::test_each_mutation[gust_below_speed] PASSED        [ 32%]
tests/test_faults.py::test_each_mutation[extra_field] PASSED             [ 33%]
tests/test_faults.py::test_each_mutation[missing_required] PASSED        [ 33%]
tests/test_faults.py::test_each_mutation[string_in_number] PASSED        [ 34%]
tests/test_faults.py::test_each_mutation[future_timestamp] PASSED        [ 35%]
tests/test_faults.py::test_each_mutation[bad_schema_version] PASSED      [ 35%]
tests/test_faults.py::test_each_mutation[float_in_integer] PASSED        [ 36%]
tests/test_faults.py::test_fault_rng_is_independent_from_weather_seed PASSED [ 37%]
tests/test_faults.py::test_same_seed_same_faults PASSED                  [ 37%]
tests/test_main.py::test_live_mode_aligned_timestamps_and_sequence PASSED [ 38%]
tests/test_main.py::test_live_timestamps_never_in_future PASSED          [ 39%]
tests/test_main.py::test_no_cumulative_drift_with_slow_requests PASSED   [ 39%]
tests/test_main.py::test_replay_then_live PASSED                         [ 40%]
tests/test_main.py::test_replay_is_spread_over_duration PASSED           [ 41%]
tests/test_main.py::test_fast_replay_larger_than_buffer_does_not_overflow PASSED [ 41%]
tests/test_main.py::test_replay_pauses_while_api_is_down PASSED          [ 42%]
tests/test_main.py::test_max_readings_waits_for_delivery PASSED          [ 43%]
tests/test_main.py::test_outage_buffers_and_delivers_in_order PASSED     [ 43%]
tests/test_main.py::test_signal_stop_drains_and_exits PASSED             [ 44%]
tests/test_main.py::test_drain_gives_up_after_timeout PASSED             [ 45%]
tests/test_main.py::test_faults_create_sequence_gaps PASSED              [ 45%]
tests/test_main.py::test_fault_rate_does_not_change_weather_end_to_end PASSED [ 46%]
tests/test_main.py::test_install_signal_handlers_sets_event PASSED       [ 47%]
tests/test_sender.py::test_classify[201-created] PASSED                  [ 47%]
tests/test_sender.py::test_classify[200-created] PASSED                  [ 48%]
tests/test_sender.py::test_classify[409-duplicate] PASSED                [ 49%]
tests/test_sender.py::test_classify[500-retry] PASSED                    [ 49%]
tests/test_sender.py::test_classify[502-retry] PASSED                    [ 50%]
tests/test_sender.py::test_classify[503-retry] PASSED                    [ 50%]
tests/test_sender.py::test_classify[408-retry] PASSED                    [ 51%]
tests/test_sender.py::test_classify[429-retry] PASSED                    [ 52%]
tests/test_sender.py::test_classify[400-rejected] PASSED                 [ 52%]
tests/test_sender.py::test_classify[401-rejected] PASSED                 [ 53%]
tests/test_sender.py::test_classify[403-rejected] PASSED                 [ 54%]
tests/test_sender.py::test_classify[404-rejected] PASSED                 [ 54%]
tests/test_sender.py::test_classify[413-rejected] PASSED                 [ 55%]
tests/test_sender.py::test_classify[415-rejected] PASSED                 [ 56%]
tests/test_sender.py::test_classify[422-rejected] PASSED                 [ 56%]
tests/test_sender.py::test_201_is_created PASSED                         [ 57%]
tests/test_sender.py::test_request_format PASSED                         [ 58%]
tests/test_sender.py::test_409_counts_as_delivered_duplicate PASSED      [ 58%]
tests/test_sender.py::test_503_then_201_keeps_order PASSED               [ 59%]
tests/test_sender.py::test_timeouts_and_transport_errors_are_retried[ReadTimeout] PASSED [ 60%]
tests/test_sender.py::test_timeouts_and_transport_errors_are_retried[ConnectTimeout] PASSED [ 60%]
tests/test_sender.py::test_timeouts_and_transport_errors_are_retried[RemoteProtocolError] PASSED [ 61%]
tests/test_sender.py::test_connection_down_with_5_buffered_flushes_in_order PASSED [ 62%]
tests/test_sender.py::test_backoff_is_exponential_with_jitter_and_capped PASSED [ 62%]
tests/test_sender.py::test_overflow_drops_oldest PASSED                  [ 63%]
tests/test_sender.py::test_non_retryable_are_dropped_without_retry[400] PASSED [ 64%]
tests/test_sender.py::test_non_retryable_are_dropped_without_retry[401] PASSED [ 64%]
tests/test_sender.py::test_non_retryable_are_dropped_without_retry[403] PASSED [ 65%]
tests/test_sender.py::test_non_retryable_are_dropped_without_retry[413] PASSED [ 66%]
tests/test_sender.py::test_non_retryable_are_dropped_without_retry[415] PASSED [ 66%]
tests/test_sender.py::test_non_retryable_are_dropped_without_retry[422] PASSED [ 67%]
tests/test_sender.py::test_401_does_not_loop PASSED                      [ 68%]
tests/test_sender.py::test_422_logs_field_code_summary PASSED            [ 68%]
tests/test_sender.py::test_fault_confirmed_and_mismatch PASSED           [ 69%]
tests/test_sender.py::test_fault_accepted_with_201_is_mismatch PASSED    [ 70%]
tests/test_sender.py::test_logs_never_contain_key_or_full_payload PASSED [ 70%]
tests/test_weather_model.py::test_payload_values_within_api_ranges_and_formats[60] PASSED [ 71%]
tests/test_weather_model.py::test_payload_values_within_api_ranges_and_formats[10] PASSED [ 72%]
tests/test_weather_model.py::test_payload_values_within_api_ranges_and_formats[300] PASSED [ 72%]
tests/test_weather_model.py::test_humidity_in_range_and_high_when_raining[60] PASSED [ 73%]
tests/test_weather_model.py::test_humidity_in_range_and_high_when_raining[10] PASSED [ 74%]
tests/test_weather_model.py::test_humidity_in_range_and_high_when_raining[300] PASSED [ 74%]
tests/test_weather_model.py::test_gust_and_direction[60] PASSED          [ 75%]
tests/test_weather_model.py::test_gust_and_direction[10] PASSED          [ 76%]
tests/test_weather_model.py::test_gust_and_direction[300] PASSED         [ 76%]
tests/test_weather_model.py::test_radiation_zero_at_night_and_bounded_by_clear_sky[60] PASSED [ 77%]
tests/test_weather_model.py::test_radiation_zero_at_night_and_bounded_by_clear_sky[10] PASSED [ 78%]
tests/test_weather_model.py::test_radiation_zero_at_night_and_bounded_by_clear_sky[300] PASSED [ 78%]
tests/test_weather_model.py::test_clear_sky_maximum_between_900_and_1200[2026-04-15] PASSED [ 79%]
tests/test_weather_model.py::test_clear_sky_maximum_between_900_and_1200[2026-10-01] PASSED [ 80%]
tests/test_weather_model.py::test_tmax_and_tmin_hours_on_dry_days[2026-04-15-60] PASSED [ 80%]
tests/test_weather_model.py::test_tmax_and_tmin_hours_on_dry_days[2026-04-15-10] PASSED [ 81%]
tests/test_weather_model.py::test_tmax_and_tmin_hours_on_dry_days[2026-04-15-300] PASSED [ 82%]
tests/test_weather_model.py::test_tmax_and_tmin_hours_on_dry_days[2026-10-01-60] PASSED [ 82%]
tests/test_weather_model.py::test_tmax_and_tmin_hours_on_dry_days[2026-10-01-10] PASSED [ 83%]
tests/test_weather_model.py::test_tmax_and_tmin_hours_on_dry_days[2026-10-01-300] PASSED [ 84%]
tests/test_weather_model.py::test_temperature_humidity_anticorrelated_on_dry_days[2026-04-15-60] PASSED [ 84%]
tests/test_weather_model.py::test_temperature_humidity_anticorrelated_on_dry_days[2026-04-15-10] PASSED [ 85%]
tests/test_weather_model.py::test_temperature_humidity_anticorrelated_on_dry_days[2026-04-15-300] PASSED [ 86%]
tests/test_weather_model.py::test_temperature_humidity_anticorrelated_on_dry_days[2026-10-01-60] PASSED [ 86%]
tests/test_weather_model.py::test_temperature_humidity_anticorrelated_on_dry_days[2026-10-01-10] PASSED [ 87%]
tests/test_weather_model.py::test_temperature_humidity_anticorrelated_on_dry_days[2026-10-01-300] PASSED [ 88%]
tests/test_weather_model.py::test_precipitation_has_no_float_artifacts PASSED [ 88%]
tests/test_weather_model.py::test_same_seed_is_byte_identical[60] PASSED [ 89%]
tests/test_weather_model.py::test_same_seed_is_byte_identical[10] PASSED [ 90%]
tests/test_weather_model.py::test_same_seed_is_byte_identical[300] PASSED [ 90%]
tests/test_weather_model.py::test_different_seeds_and_stations_differ PASSED [ 91%]
tests/test_weather_model.py::test_fault_rate_does_not_change_unmutated_measurements PASSED [ 92%]
tests/test_weather_model.py::test_precipitation_accumulates_per_interval[10] PASSED [ 92%]
tests/test_weather_model.py::test_precipitation_accumulates_per_interval[60] PASSED [ 93%]
tests/test_weather_model.py::test_precipitation_accumulates_per_interval[300] PASSED [ 94%]
tests/test_weather_model.py::test_precipitation_accumulates_per_interval[3600] PASSED [ 94%]
tests/test_weather_model.py::test_ar1_variance_and_correlation_do_not_depend_on_dt[10] PASSED [ 95%]
tests/test_weather_model.py::test_ar1_variance_and_correlation_do_not_depend_on_dt[60] PASSED [ 96%]
tests/test_weather_model.py::test_ar1_variance_and_correlation_do_not_depend_on_dt[300] PASSED [ 96%]
tests/test_weather_model.py::test_ar1_variance_and_correlation_do_not_depend_on_dt[3600] PASSED [ 97%]
tests/test_weather_model.py::test_battery_charges_by_day_and_drains_at_night PASSED [ 98%]
tests/test_weather_model.py::test_magnus_relative_humidity PASSED        [ 98%]
tests/test_weather_model.py::test_generator_sequence_and_timestamp PASSED [ 99%]
tests/test_weather_model.py::test_model_requires_advancing_aware_time PASSED [100%]

============================= slowest 5 durations =============================
39.56s call     tests/test_weather_model.py::test_payload_values_within_api_ranges_and_formats[10]
7.62s call     tests/test_weather_model.py::test_payload_values_within_api_ranges_and_formats[60]
1.34s call     tests/test_weather_model.py::test_payload_values_within_api_ranges_and_formats[300]
0.73s call     tests/test_config.py::test_invalid_config_exits_2_without_printing_key[PREVAILING_WIND_DEG=360]
0.73s call     tests/test_config.py::test_invalid_config_exits_2_without_printing_key[INGEST_URL=no-es-una-url]
======================= 151 passed in 67.50s (0:01:07) ========================
exit 0
```

### ingest_api

Comando: `cd ingest_api; ..\.venv\Scripts\python.exe -m pytest -v` (la última línea, `exit N`, es el código de salida que agregó el script).

```
============================= test session starts =============================
platform win32 -- Python 3.13.5, pytest-9.1.1, pluggy-1.6.0 -- C:\Users\orizo\OneDrive\Documentos\TEC\SKILLIKET\.venv\Scripts\python.exe
cachedir: .pytest_cache
rootdir: C:\Users\orizo\OneDrive\Documentos\TEC\SKILLIKET\ingest_api
configfile: pytest.ini
testpaths: tests
plugins: anyio-4.15.1
collecting ... collected 98 items

tests/test_auth.py::test_both_station_keys_resolve_to_station PASSED     [  1%]
tests/test_auth.py::test_unknown_or_missing_key_is_rejected[None] PASSED [  2%]
tests/test_auth.py::test_unknown_or_missing_key_is_rejected[] PASSED     [  3%]
tests/test_auth.py::test_unknown_or_missing_key_is_rejected[k4] PASSED   [  4%]
tests/test_auth.py::test_wrong_hash_rejects_its_key PASSED               [  5%]
tests/test_auth.py::test_parse_valid_hashes PASSED                       [  6%]
tests/test_auth.py::test_invalid_hash_config_is_rejected[not-json] PASSED [  7%]
tests/test_auth.py::test_invalid_hash_config_is_rejected[empty] PASSED   [  8%]
tests/test_auth.py::test_invalid_hash_config_is_rejected[not-object] PASSED [  9%]
tests/test_auth.py::test_invalid_hash_config_is_rejected[bad-station-id] PASSED [ 10%]
tests/test_auth.py::test_invalid_hash_config_is_rejected[unicode-digit-station-id] PASSED [ 11%]
tests/test_auth.py::test_invalid_hash_config_is_rejected[no-hashes] PASSED [ 12%]
tests/test_auth.py::test_invalid_hash_config_is_rejected[three-hashes] PASSED [ 13%]
tests/test_auth.py::test_invalid_hash_config_is_rejected[uppercase-hex] PASSED [ 14%]
tests/test_auth.py::test_invalid_hash_config_is_rejected[short-hash] PASSED [ 15%]
tests/test_auth.py::test_invalid_hash_config_is_rejected[repeated-in-station] PASSED [ 16%]
tests/test_auth.py::test_invalid_hash_config_is_rejected[shared-across-stations] PASSED [ 17%]
tests/test_auth.py::test_invalid_hash_config_is_rejected[not-list] PASSED [ 18%]
tests/test_auth.py::test_settings_require_station_hashes PASSED          [ 19%]
tests/test_auth.py::test_settings_from_env PASSED                        [ 20%]
tests/test_gen_station_key.py::test_generated_key_is_32_bytes_urlsafe_and_hash_matches_api PASSED [ 21%]
tests/test_models.py::test_exported_schema_matches_model PASSED          [ 22%]
tests/test_models.py::test_valid_payload_parses PASSED                   [ 23%]
tests/test_models.py::test_minimal_payload_without_optional_fields PASSED [ 24%]
tests/test_models.py::test_direction_is_not_tied_to_speed[calm-null-direction] PASSED [ 25%]
tests/test_models.py::test_direction_is_not_tied_to_speed[low-speed-numeric-direction] PASSED [ 26%]
tests/test_models.py::test_gust_equal_to_speed_is_valid PASSED           [ 27%]
tests/test_models.py::test_patterns_reject_non_ascii_digits[station-id] PASSED [ 28%]
tests/test_models.py::test_patterns_reject_non_ascii_digits[timestamp] PASSED [ 29%]
tests/test_models.py::test_invalid_payloads[rh-101] PASSED               [ 30%]
tests/test_models.py::test_invalid_payloads[temp-minus-41] PASSED        [ 31%]
tests/test_models.py::test_invalid_payloads[gust-below-speed] PASSED     [ 32%]
tests/test_models.py::test_invalid_payloads[extra-field] PASSED          [ 33%]
tests/test_models.py::test_invalid_payloads[dew-point-present] PASSED    [ 34%]
tests/test_models.py::test_invalid_payloads[missing-required] PASSED     [ 35%]
tests/test_models.py::test_invalid_payloads[timestamp-10min-future] PASSED [ 36%]
tests/test_models.py::test_invalid_payloads[timestamp-8days-past] PASSED [ 37%]
tests/test_models.py::test_invalid_payloads[timestamp-offset-not-z] PASSED [ 38%]
tests/test_models.py::test_invalid_payloads[station-id-format] PASSED    [ 39%]
tests/test_models.py::test_invalid_payloads[schema-version-2] PASSED     [ 40%]
tests/test_models.py::test_invalid_payloads[string-in-number] PASSED     [ 41%]
tests/test_models.py::test_invalid_payloads[bool-in-number] PASSED       [ 42%]
tests/test_models.py::test_invalid_payloads[interval-too-short] PASSED   [ 43%]
tests/test_models.py::test_invalid_payloads[negative-sequence] PASSED    [ 44%]
tests/test_models.py::test_invalid_payloads[direction-360] PASSED        [ 45%]
tests/test_models.py::test_invalid_payloads[device-extra] PASSED         [ 46%]
tests/test_models.py::test_invalid_payloads[interval-float] PASSED       [ 47%]
tests/test_models.py::test_invalid_payloads[sequence-float] PASSED       [ 48%]
tests/test_models.py::test_invalid_payloads[station-id-unicode-digits] PASSED [ 50%]
tests/test_models.py::test_invalid_payloads[timestamp-fullwidth-digits] PASSED [ 51%]
tests/test_models.py::test_window_uses_validation_context PASSED         [ 52%]
tests/test_readings.py::test_valid_payload_is_created_and_persisted PASSED [ 53%]
tests/test_readings.py::test_duplicate_is_409 PASSED                     [ 54%]
tests/test_readings.py::test_duplicate_detection_uses_instant_not_text PASSED [ 55%]
tests/test_readings.py::test_index_is_rebuilt_after_restart PASSED       [ 56%]
tests/test_readings.py::test_key_inside_window_survives_pruning PASSED   [ 57%]
tests/test_readings.py::test_low_wind_direction_cases_are_created[null-direction] PASSED [ 58%]
tests/test_readings.py::test_low_wind_direction_cases_are_created[numeric-direction] PASSED [ 59%]
tests/test_readings.py::test_missing_key_is_401 PASSED                   [ 60%]
tests/test_readings.py::test_invalid_key_is_401 PASSED                   [ 61%]
tests/test_readings.py::test_both_keys_of_a_station_are_accepted PASSED  [ 62%]
tests/test_readings.py::test_key_of_other_station_is_403 PASSED          [ 63%]
tests/test_readings.py::test_unsupported_content_type_is_415[text/plain] PASSED [ 64%]
tests/test_readings.py::test_unsupported_content_type_is_415[application/xml] PASSED [ 65%]
tests/test_readings.py::test_unsupported_content_type_is_415[application/json; charset=latin-1] PASSED [ 66%]
tests/test_readings.py::test_json_with_utf8_charset_is_accepted PASSED   [ 67%]
tests/test_readings.py::test_body_over_4kb_is_413 PASSED                 [ 68%]
tests/test_readings.py::test_chunked_body_over_4kb_without_content_length_is_413 PASSED [ 69%]
tests/test_readings.py::test_body_of_4097_bytes_is_413 PASSED            [ 70%]
tests/test_readings.py::test_body_of_exactly_4096_bytes_is_not_413 PASSED [ 71%]
tests/test_readings.py::test_malformed_json_is_400_with_position[missing-value] PASSED [ 72%]
tests/test_readings.py::test_malformed_json_is_400_with_position[empty-body] PASSED [ 73%]
tests/test_readings.py::test_malformed_json_is_400_with_position[nan] PASSED [ 74%]
tests/test_readings.py::test_invalid_utf8_is_400 PASSED                  [ 75%]
tests/test_readings.py::test_deeply_nested_json_is_400 PASSED            [ 76%]
tests/test_readings.py::test_schema_error_is_422_with_field_errors PASSED [ 77%]
tests/test_readings.py::test_missing_field_error_does_not_echo_payload PASSED [ 78%]
tests/test_readings.py::test_non_object_body_is_422 PASSED               [ 79%]
tests/test_readings.py::test_timestamp_window_comes_from_settings PASSED [ 80%]
tests/test_readings.py::test_order_401_before_415_413_400 PASSED         [ 81%]
tests/test_readings.py::test_order_415_before_413 PASSED                 [ 82%]
tests/test_readings.py::test_order_413_before_400 PASSED                 [ 83%]
tests/test_readings.py::test_order_422_before_403 PASSED                 [ 84%]
tests/test_readings.py::test_order_403_before_409 PASSED                 [ 85%]
tests/test_readings.py::test_request_validation_handler_separates_400_and_422 PASSED [ 86%]
tests/test_readings.py::test_unknown_route_is_404_problem PASSED         [ 87%]
tests/test_readings.py::test_wrong_method_is_405_problem_with_allow PASSED [ 88%]
tests/test_readings.py::test_unexpected_error_is_generic_500 PASSED      [ 89%]
tests/test_readings.py::test_health_docs_and_openapi PASSED              [ 90%]
tests/test_readings.py::test_logs_have_ids_and_never_the_key_or_payload PASSED [ 91%]
tests/test_storage.py::test_concurrent_distinct_saves_write_whole_lines PASSED [ 92%]
tests/test_storage.py::test_concurrent_same_reading_saves_once PASSED    [ 93%]
tests/test_storage.py::test_index_rebuild_skips_corrupt_lines_and_keeps_appending_cleanly PASSED [ 94%]
tests/test_storage.py::test_partial_write_failure_does_not_glue_next_reading PASSED [ 95%]
tests/test_storage.py::test_prune_drops_keys_outside_retention PASSED    [ 96%]
tests/test_storage.py::test_prune_runs_at_most_every_5_minutes PASSED    [ 97%]
tests/test_storage.py::test_rebuild_loads_only_keys_inside_retention PASSED [ 98%]
tests/test_storage.py::test_record_format PASSED                         [100%]

============================= 98 passed in 2.08s ==============================
exit 0
```

### contract_tests

Comando: `cd contract_tests; ..\.venv\Scripts\python.exe -m pytest -v` (la última línea, `exit N`, es el código de salida que agregó el script).

```
============================= test session starts =============================
platform win32 -- Python 3.13.5, pytest-9.1.1, pluggy-1.6.0 -- C:\Users\orizo\OneDrive\Documentos\TEC\SKILLIKET\.venv\Scripts\python.exe
cachedir: .pytest_cache
rootdir: C:\Users\orizo\OneDrive\Documentos\TEC\SKILLIKET\contract_tests
configfile: pytest.ini
testpaths: .
plugins: anyio-4.15.1
collecting ... collected 12 items

test_contract.py::test_all_4320_payloads_are_valid PASSED                [  8%]
test_contract.py::test_stations_produce_different_series PASSED          [ 16%]
test_contract.py::test_each_fault_fails_with_expected_field_and_code[humidity_over_100] PASSED [ 25%]
test_contract.py::test_each_fault_fails_with_expected_field_and_code[temperature_below_min] PASSED [ 33%]
test_contract.py::test_each_fault_fails_with_expected_field_and_code[gust_below_speed] PASSED [ 41%]
test_contract.py::test_each_fault_fails_with_expected_field_and_code[extra_field] PASSED [ 50%]
test_contract.py::test_each_fault_fails_with_expected_field_and_code[missing_required] PASSED [ 58%]
test_contract.py::test_each_fault_fails_with_expected_field_and_code[string_in_number] PASSED [ 66%]
test_contract.py::test_each_fault_fails_with_expected_field_and_code[future_timestamp] PASSED [ 75%]
test_contract.py::test_each_fault_fails_with_expected_field_and_code[bad_schema_version] PASSED [ 83%]
test_contract.py::test_each_fault_fails_with_expected_field_and_code[float_in_integer] PASSED [ 91%]
test_contract.py::test_injector_rate_1_over_all_payloads_matches_catalog PASSED [100%]

============================= 12 passed in 1.45s ==============================
exit 0
```
````
