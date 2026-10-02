import json

import pytest

from app.auth import KeyVerifier, hash_key
from app.config import ConfigError, load_settings, parse_station_key_hashes

H1, H2, H3 = (hash_key(k) for k in ("a", "b", "c"))


def test_both_station_keys_resolve_to_station() -> None:
    verifier = KeyVerifier({"GDL-001": (hash_key("k1"), hash_key("k2")), "GDL-002": (hash_key("k3"),)})
    assert verifier.station_for_key("k1") == "GDL-001"
    assert verifier.station_for_key("k2") == "GDL-001"
    assert verifier.station_for_key("k3") == "GDL-002"


@pytest.mark.parametrize("key", [None, "", "k4"])
def test_unknown_or_missing_key_is_rejected(key) -> None:
    assert KeyVerifier({"GDL-001": (hash_key("k1"),)}).station_for_key(key) is None


def test_wrong_hash_rejects_its_key() -> None:
    wrong = hash_key("k1")[:-1] + ("0" if hash_key("k1")[-1] != "0" else "1")
    assert KeyVerifier({"GDL-001": (wrong,)}).station_for_key("k1") is None


def test_parse_valid_hashes() -> None:
    assert parse_station_key_hashes(json.dumps({"GDL-001": [H1, H2]})) == {"GDL-001": (H1, H2)}


@pytest.mark.parametrize(
    "raw",
    [
        "not json",
        "{}",
        "[]",
        json.dumps({"gdl-001": [H1]}),
        json.dumps({"GDL-٠٠١": [H1]}),
        json.dumps({"GDL-001": []}),
        json.dumps({"GDL-001": [H1, H2, H3]}),
        json.dumps({"GDL-001": [H1.upper()]}),
        json.dumps({"GDL-001": ["abc"]}),
        json.dumps({"GDL-001": [H1, H1]}),
        json.dumps({"GDL-001": [H1], "GDL-002": [H1]}),
        json.dumps({"GDL-001": H1}),
    ],
    ids=[
        "not-json", "empty", "not-object", "bad-station-id", "unicode-digit-station-id",
        "no-hashes", "three-hashes",
        "uppercase-hex", "short-hash", "repeated-in-station", "shared-across-stations", "not-list",
    ],
)
def test_invalid_hash_config_is_rejected(raw: str) -> None:
    with pytest.raises(ConfigError):
        parse_station_key_hashes(raw)


def test_settings_require_station_hashes(monkeypatch) -> None:
    monkeypatch.delenv("STATION_KEY_HASHES", raising=False)
    with pytest.raises(ConfigError):
        load_settings()


def test_settings_from_env(monkeypatch) -> None:
    monkeypatch.setenv("STATION_KEY_HASHES", json.dumps({"GDL-001": [H1]}))
    monkeypatch.setenv("MAX_FUTURE_SKEW_S", "60")
    monkeypatch.delenv("STORAGE_PATH", raising=False)
    monkeypatch.delenv("MAX_PAST_AGE_S", raising=False)
    settings = load_settings()
    assert settings.max_future_skew.total_seconds() == 60
    assert settings.max_past_age.total_seconds() == 7 * 24 * 3600
    assert settings.storage_path.as_posix() == "data/readings.jsonl"
