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
