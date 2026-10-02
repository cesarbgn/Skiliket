"""Regenera schema/weather_reading.v1.schema.json desde el modelo Pydantic.

Uso: python scripts/export_schema.py
"""

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = ROOT / "schema" / "weather_reading.v1.schema.json"

sys.path.insert(0, str(ROOT / "ingest_api"))

from app.models import weather_reading_json_schema  # noqa: E402


def render() -> str:
    return json.dumps(weather_reading_json_schema(), indent=2, ensure_ascii=False) + "\n"


def main() -> None:
    SCHEMA_PATH.parent.mkdir(parents=True, exist_ok=True)
    SCHEMA_PATH.write_text(render(), encoding="utf-8", newline="\n")
    print(f"Escrito {SCHEMA_PATH.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
