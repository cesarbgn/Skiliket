"""Autenticación de estaciones por API key (cabecera X-Station-Key)."""

import hashlib
import hmac
from collections.abc import Mapping, Sequence

STATION_KEY_HEADER = "X-Station-Key"


def hash_key(key: str) -> str:
    """SHA-256 en hex minúsculas; es lo único que se configura y se guarda."""
    return hashlib.sha256(key.encode("utf-8")).hexdigest()


class KeyVerifier:
    def __init__(self, station_key_hashes: Mapping[str, Sequence[str]]) -> None:
        self._entries = [
            (station_id, digest.encode("ascii"))
            for station_id, digests in station_key_hashes.items()
            for digest in digests
        ]

    def station_for_key(self, key: str | None) -> str | None:
        """Devuelve la estación dueña de la clave, o None si no es válida."""
        if not key:
            return None
        candidate = hash_key(key).encode("ascii")
        match = None
        # Se recorren todas las entradas (sin salir antes) para no filtrar por tiempo cuál coincidió.
        for station_id, digest in self._entries:
            if hmac.compare_digest(candidate, digest):
                match = station_id
        return match
