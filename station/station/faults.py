"""Inyección de fallas: muta un payload ya generado para provocar un 422 conocido.

Usa su propio random.Random, independiente del clima: cambiar FAULT_RATE no altera los
valores meteorológicos de las lecturas que no se mutan."""

import copy
import hashlib
import random
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from station.weather_model import format_timestamp

Payload = dict[str, Any]


@dataclass(frozen=True)
class Fault:
    name: str
    field: str  # campo esperado en errors[].field del 422
    code: str  # código esperado en errors[].code
    apply: Callable[[Payload, datetime], None]


def _humidity_over_100(p: Payload, now: datetime) -> None:
    p["measurements"]["relative_humidity_pct"] = 101


def _temperature_below_min(p: Payload, now: datetime) -> None:
    p["measurements"]["air_temperature_c"] = -41


def _gust_below_speed(p: Payload, now: datetime) -> None:
    m = p["measurements"]
    m["wind_speed_ms"] = max(1.0, m["wind_speed_ms"])
    m["wind_gust_ms"] = round(m["wind_speed_ms"] - 0.5, 1)


def _extra_field(p: Payload, now: datetime) -> None:
    p["unexpected_field"] = 1


def _missing_required(p: Payload, now: datetime) -> None:
    del p["measurements"]["station_pressure_hpa"]


def _string_in_number(p: Payload, now: datetime) -> None:
    p["measurements"]["air_temperature_c"] = str(p["measurements"]["air_temperature_c"])


def _future_timestamp(p: Payload, now: datetime) -> None:
    # +10 min sobre el reloj actual, no sobre el timestamp de la lectura: en réplica o con
    # intervalos largos, timestamp + 10 min puede seguir en el pasado y el API lo aceptaría.
    p["timestamp"] = format_timestamp(now + timedelta(minutes=10))


def _bad_schema_version(p: Payload, now: datetime) -> None:
    p["schema_version"] = "2.0"


def _float_in_integer(p: Payload, now: datetime) -> None:
    p["interval_s"] = float(p["interval_s"])  # 60 -> 60.0


CATALOG: tuple[Fault, ...] = (
    Fault("humidity_over_100", "measurements.relative_humidity_pct", "less_than_equal", _humidity_over_100),
    Fault("temperature_below_min", "measurements.air_temperature_c", "greater_than_equal", _temperature_below_min),
    Fault("gust_below_speed", "measurements.wind_gust_ms", "gust_less_than_speed", _gust_below_speed),
    Fault("extra_field", "unexpected_field", "extra_forbidden", _extra_field),
    Fault("missing_required", "measurements.station_pressure_hpa", "missing", _missing_required),
    Fault("string_in_number", "measurements.air_temperature_c", "float_type", _string_in_number),
    Fault("future_timestamp", "timestamp", "timestamp_in_future", _future_timestamp),
    Fault("bad_schema_version", "schema_version", "literal_error", _bad_schema_version),
    Fault("float_in_integer", "interval_s", "int_type", _float_in_integer),
)
BY_NAME = {fault.name: fault for fault in CATALOG}


def fault_seed(seed: int) -> int:
    """Semilla de las fallas derivada de la del clima, pero en otro flujo aleatorio."""
    return int.from_bytes(hashlib.sha256(f"faults:{seed}".encode("ascii")).digest()[:8], "big")


class FaultInjector:
    def __init__(self, rate: float, seed: int) -> None:
        self.rate = rate
        self._rng = random.Random(fault_seed(seed))

    def maybe_inject(self, payload: Payload, now: datetime | None = None) -> tuple[Payload, Fault | None]:
        """Devuelve (payload, falla). Con falla, el payload es una copia mutada."""
        if self._rng.random() >= self.rate:
            return payload, None
        fault = self._rng.choice(CATALOG)
        mutated = copy.deepcopy(payload)
        fault.apply(mutated, now or datetime.now(UTC))
        return mutated, fault
