"""Genera una clave de estación: 32 bytes aleatorios en base64 urlsafe.

Imprime la clave en claro UNA sola vez y su hash SHA-256. Solo el hash va en
STATION_KEY_HASHES; la clave se entrega a la estación y no se guarda en ningún archivo.

Uso: python scripts/gen_station_key.py
"""

import hashlib
import secrets
import sys


def main() -> None:
    key = secrets.token_urlsafe(32)
    digest = hashlib.sha256(key.encode("utf-8")).hexdigest()
    print(f"STATION_KEY={key}")
    print(f"SHA256={digest}")
    print("Copia la clave ahora: no se vuelve a mostrar ni se guarda.", file=sys.stderr)


if __name__ == "__main__":
    main()
