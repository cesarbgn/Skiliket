"""Logs estructurados (una línea JSON por evento). Nunca incluyen claves ni payloads."""

import json
import logging
import sys
from datetime import UTC, datetime

_EXTRA_FIELDS = ("event", "reading_id", "station_id", "status", "reason", "codes", "line")


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
