import base64
import subprocess
import sys
from pathlib import Path

from app.auth import hash_key

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "gen_station_key.py"


def test_generated_key_is_32_bytes_urlsafe_and_hash_matches_api() -> None:
    result = subprocess.run(
        [sys.executable, str(SCRIPT)], capture_output=True, text=True, check=True
    )
    lines = dict(line.split("=", 1) for line in result.stdout.splitlines())
    assert set(lines) == {"STATION_KEY", "SHA256"}
    key = lines["STATION_KEY"]
    assert len(base64.urlsafe_b64decode(key + "=" * (-len(key) % 4))) == 32
    assert lines["SHA256"] == hash_key(key)
    assert result.stdout.count(key) == 1
