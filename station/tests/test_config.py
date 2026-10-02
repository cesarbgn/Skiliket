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
