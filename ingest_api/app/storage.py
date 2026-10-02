"""Persistencia de lecturas validadas."""

import json
import logging
import os
import threading
from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from app.models import DEFAULT_MAX_PAST_AGE, WeatherReading

logger = logging.getLogger("skilliket.ingest.storage")

DedupKey = tuple[str, datetime]

# Margen sobre max_past_age antes de olvidar una clave, y frecuencia máxima de la poda.
RETENTION_MARGIN = timedelta(hours=1)
PRUNE_INTERVAL = timedelta(minutes=5)


class DuplicateReadingError(Exception):
    """Ya existe una lectura con el mismo (station_id, timestamp)."""


@dataclass(frozen=True)
class StoredReading:
    reading_id: str
    received_at: datetime
    reading: WeatherReading

    @property
    def dedup_key(self) -> DedupKey:
        return (self.reading.station_id, self.reading.timestamp)

    def to_record(self) -> dict[str, Any]:
        return {
            "reading_id": self.reading_id,
            "received_at": self.received_at.isoformat().replace("+00:00", "Z"),
            "reading": self.reading.model_dump(mode="json", exclude_unset=True),
        }


class StorageAdapter(ABC):
    @abstractmethod
    def save(self, stored: StoredReading) -> None:
        """Persiste la lectura de forma síncrona.

        Lanza DuplicateReadingError si (station_id, timestamp) ya existe."""

    def close(self) -> None:
        """Libera recursos; opcional para implementaciones que no tienen."""


class JsonlStorage(StorageAdapter):
    """Una línea JSON por lectura en un archivo local.

    El índice de duplicados vive en memoria y se reconstruye leyendo el archivo al
    arrancar. Solo guarda claves dentro de la retención (max_past_age + margen): una
    clave más vieja es inalcanzable porque su timestamp ya se rechaza con 422.
    Válido solo para un proceso: varios workers no comparten el lock ni el índice."""

    def __init__(
        self,
        path: Path,
        max_past_age: timedelta = DEFAULT_MAX_PAST_AGE,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._path = path
        self._lock = threading.Lock()
        self._clock = clock
        self._retention = max_past_age + RETENTION_MARGIN
        self._last_prune = clock()
        # True tras un fallo de escritura: puede haber quedado una línea parcial sin salto.
        self._dirty = False
        path.parent.mkdir(parents=True, exist_ok=True)
        self._index: set[DedupKey] = self._rebuild_index()
        needs_newline = self._ends_without_newline()
        self._file = open(path, "ab")  # se cierra en close()
        if needs_newline:
            # Una última línea truncada (p. ej. tras un corte) no debe pegarse con la siguiente.
            self._file.write(b"\n")
            self._file.flush()

    @property
    def indexed_count(self) -> int:
        return len(self._index)

    def _rebuild_index(self) -> set[DedupKey]:
        index: set[DedupKey] = set()
        if not self._path.exists():
            return index
        cutoff = self._clock() - self._retention
        with open(self._path, "rb") as f:
            for lineno, line in enumerate(f, start=1):
                if not line.strip():
                    continue
                try:
                    reading = json.loads(line)["reading"]
                    key = (reading["station_id"], datetime.fromisoformat(reading["timestamp"]))
                    if key[1] >= cutoff:
                        index.add(key)
                except (ValueError, KeyError, TypeError):
                    logger.warning(
                        "storage_line_skipped", extra={"event": "storage_line_skipped", "line": lineno}
                    )
        return index

    def _prune_locked(self, now: datetime) -> None:
        """Quita las claves fuera de la retención. Requiere tener el lock."""
        cutoff = now - self._retention
        self._index = {key for key in self._index if key[1] >= cutoff}
        self._last_prune = now

    def _ends_without_newline(self) -> bool:
        if not self._path.exists() or self._path.stat().st_size == 0:
            return False
        with open(self._path, "rb") as f:
            f.seek(-1, os.SEEK_END)
            return f.read(1) != b"\n"

    def save(self, stored: StoredReading) -> None:
        line = json.dumps(stored.to_record(), ensure_ascii=False, separators=(",", ":"))
        data = line.encode("utf-8") + b"\n"
        with self._lock:
            now = self._clock()
            # La poda recorre todo el índice: como máximo una vez por intervalo, no en cada escritura.
            if now - self._last_prune >= PRUNE_INTERVAL:
                self._prune_locked(now)
            if stored.dedup_key in self._index:
                raise DuplicateReadingError
            if self._dirty:
                # Cierra la posible línea parcial del fallo anterior para no pegarse a ella.
                data = b"\n" + data
            # Una sola escritura por línea completa bajo el lock: sin intercalado entre hilos.
            try:
                self._file.write(data)
                self._file.flush()
                os.fsync(self._file.fileno())
            except BaseException:
                self._dirty = True
                raise
            self._dirty = False
            self._index.add(stored.dedup_key)

    def close(self) -> None:
        with self._lock:
            if not self._file.closed:
                self._file.close()
