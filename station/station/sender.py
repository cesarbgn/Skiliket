"""Envío de lecturas: POST con clasificación de respuestas, búfer FIFO acotado y backoff.

Política:
- 2xx (201) y 409 = entregada (409: ya estaba guardada; se cuenta como duplicate).
- 5xx, 408, 429, timeouts y errores de conexión = reintentable: la lectura se queda al
  frente del búfer y se reintenta con backoff exponencial con jitter (tope 60 s).
- Cualquier otro 4xx (400, 401, 403, 413, 415, 422...) = no reintentable: se descarta.
Los logs llevan station_id, sequence, status, intento y un resumen del problem+json
(type y pares field/code); nunca el payload completo ni la clave."""

import json
import logging
import random
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Literal

import httpx

from station.config import Secret
from station.faults import Fault

logger = logging.getLogger("skilliket.station")

STATION_KEY_HEADER = "X-Station-Key"
BACKOFF_BASE_S = 1.0
BACKOFF_MAX_S = 60.0
_MAX_ERRORS_LOGGED = 20
_MAX_TEXT = 100

Outcome = Literal["created", "duplicate", "retry", "rejected"]


@dataclass
class Pending:
    payload: dict[str, Any]
    fault: Fault | None = None
    attempts: int = 0

    @property
    def sequence(self) -> int | None:
        return self.payload.get("sequence")

    @property
    def timestamp(self) -> Any:
        return self.payload.get("timestamp")


@dataclass
class Stats:
    generated: int = 0
    created: int = 0
    duplicates: int = 0
    dropped_by_status: dict[str, int] = field(default_factory=dict)
    dropped_overflow: int = 0
    retries: int = 0
    faults_injected: int = 0
    faults_confirmed: int = 0
    faults_unexpected: int = 0
    unsent_at_exit: int = 0

    def as_dict(self) -> dict[str, Any]:
        return {
            "generated": self.generated,
            "created": self.created,
            "duplicates": self.duplicates,
            "dropped_by_status": dict(sorted(self.dropped_by_status.items())),
            "dropped_overflow": self.dropped_overflow,
            "retries": self.retries,
            "faults_injected": self.faults_injected,
            "faults_confirmed": self.faults_confirmed,
            "faults_unexpected": self.faults_unexpected,
            "unsent_at_exit": self.unsent_at_exit,
        }


@dataclass(frozen=True)
class Attempt:
    outcome: Outcome
    status: int | None = None
    error_type: str | None = None  # clase de la excepción de transporte
    problem_type: str | None = None
    errors: tuple[tuple[str, str], ...] = ()  # (field, code) del 422


def _short(value: Any) -> str:
    return str(value)[:_MAX_TEXT]


def _problem_summary(response: httpx.Response) -> tuple[str | None, tuple[tuple[str, str], ...]]:
    """type y pares (field, code) del problem+json; nunca los valores devueltos."""
    try:
        body = response.json()
    except ValueError:
        return None, ()
    if not isinstance(body, dict):
        return None, ()
    problem_type = _short(body["type"]) if isinstance(body.get("type"), str) else None
    pairs = []
    errors = body.get("errors")
    if isinstance(errors, list):
        for error in errors[:_MAX_ERRORS_LOGGED]:
            if isinstance(error, dict):
                pairs.append((_short(error.get("field")), _short(error.get("code"))))
    return problem_type, tuple(pairs)


def classify(status: int) -> Outcome:
    if 200 <= status < 300:
        return "created"
    if status == 409:
        return "duplicate"
    if status in (408, 429) or status >= 500:
        return "retry"
    return "rejected"


class Sender:
    """Un POST por llamada; nunca lanza por errores de red: los devuelve como Attempt."""

    def __init__(self, client: httpx.Client, url: str, key: Secret) -> None:
        self._client = client
        self._url = url
        self._key = key

    def post(self, payload: dict[str, Any], timeout: float | None = None) -> Attempt:
        body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        headers = {"Content-Type": "application/json", STATION_KEY_HEADER: self._key.reveal()}
        kwargs: dict[str, Any] = {}
        if timeout is not None:
            kwargs["timeout"] = httpx.Timeout(max(0.1, timeout))
        try:
            response = self._client.post(self._url, content=body, headers=headers, **kwargs)
        except httpx.RequestError as exc:
            # Solo el tipo: el mensaje de algunas excepciones incluye la URL o el request.
            return Attempt("retry", error_type=type(exc).__name__)
        outcome = classify(response.status_code)
        problem_type, errors = (None, ())
        if outcome in ("rejected", "retry", "duplicate"):
            problem_type, errors = _problem_summary(response)
        return Attempt(outcome, response.status_code, problem_type=problem_type, errors=errors)


class Outbox:
    """Búfer FIFO acotado. El frente se reintenta hasta entregarse o descartarse, así que
    el orden de entrega se conserva. Si se llena, se descarta la lectura más vieja."""

    def __init__(
        self,
        sender: Sender,
        station_id: str,
        max_size: int,
        stats: Stats,
        monotonic: Callable[[], float],
        jitter: random.Random | None = None,
    ) -> None:
        self._sender = sender
        self._station_id = station_id
        self._max_size = max_size
        self._stats = stats
        self._monotonic = monotonic
        self._jitter = jitter or random.Random()
        self._queue: deque[Pending] = deque()
        self._failures = 0  # fallos reintentables seguidos
        self.retry_at = 0.0  # instante monotónico a partir del cual se puede enviar

    def __len__(self) -> int:
        return len(self._queue)

    def sequences(self) -> list[int | None]:
        return [p.sequence for p in self._queue]

    def put(self, pending: Pending) -> None:
        if len(self._queue) >= self._max_size:
            dropped = self._queue.popleft()
            self._stats.dropped_overflow += 1
            logger.warning(
                "buffer_overflow",
                extra={
                    "event": "buffer_overflow",
                    "station_id": self._station_id,
                    "dropped_sequence": dropped.sequence,
                    "buffered": len(self._queue),
                },
            )
        self._queue.append(pending)

    def retry_now(self) -> None:
        self.retry_at = 0.0

    def backoff_delay(self) -> float:
        base = min(BACKOFF_MAX_S, BACKOFF_BASE_S * 2 ** (self._failures - 1))
        return base * (0.5 + 0.5 * self._jitter.random())

    def send_next(self, timeout: float | None = None) -> Attempt | None:
        """Intenta enviar el frente del búfer. Devuelve el resultado o None si está vacío."""
        if not self._queue:
            return None
        pending = self._queue[0]
        pending.attempts += 1
        attempt = self._sender.post(pending.payload, timeout)
        if attempt.outcome == "retry":
            self._failures += 1
            self._stats.retries += 1
            delay = self.backoff_delay()
            self.retry_at = self._monotonic() + delay
            logger.warning(
                "send_retry",
                extra={
                    "event": "send_retry",
                    "station_id": self._station_id,
                    "sequence": pending.sequence,
                    "status": attempt.status,
                    "error_type": attempt.error_type,
                    "attempt": pending.attempts,
                    "delay_s": round(delay, 2),
                    "buffered": len(self._queue),
                },
            )
            return attempt

        self._queue.popleft()
        self._failures = 0
        self.retry_at = 0.0
        if attempt.outcome == "created":
            self._stats.created += 1
        elif attempt.outcome == "duplicate":
            self._stats.duplicates += 1
        else:
            key = str(attempt.status)
            self._stats.dropped_by_status[key] = self._stats.dropped_by_status.get(key, 0) + 1
        self._log_result(pending, attempt)
        if pending.fault is not None:
            self._check_fault(pending, pending.fault, attempt)
        return attempt

    def _log_result(self, pending: Pending, attempt: Attempt) -> None:
        extra: dict[str, Any] = {
            "event": "reading",
            "station_id": self._station_id,
            "sequence": pending.sequence,
            "timestamp": pending.timestamp,
            "status": attempt.status,
            "attempt": pending.attempts,
            "outcome": attempt.outcome,
        }
        if attempt.outcome in ("created", "duplicate"):
            logger.info("reading_delivered", extra=extra)
            return
        extra["problem_type"] = attempt.problem_type
        extra["errors"] = [list(pair) for pair in attempt.errors]
        if attempt.status == 401:
            extra["reason"] = "check_station_key"
            logger.error(
                "reading_rejected: 401, el API no reconoce STATION_KEY; revisa la clave "
                "(y que su hash esté en STATION_KEY_HASHES del API). No se reintenta.",
                extra=extra,
            )
        elif attempt.status == 403:
            extra["reason"] = "check_station_id"
            logger.error(
                "reading_rejected: 403, la clave pertenece a otra estación; revisa STATION_ID "
                "y STATION_KEY. No se reintenta.",
                extra=extra,
            )
        else:
            logger.warning("reading_rejected", extra=extra)

    def _check_fault(self, pending: Pending, fault: Fault, attempt: Attempt) -> None:
        expected = [fault.field, fault.code]
        received = {"status": attempt.status, "errors": [list(pair) for pair in attempt.errors]}
        extra = {
            "event": "fault_check",
            "station_id": self._station_id,
            "sequence": pending.sequence,
            "fault": fault.name,
            "expected": {"status": 422, "errors": [expected]},
            "received": received,
        }
        if attempt.status == 422 and set(attempt.errors) == {(fault.field, fault.code)}:
            self._stats.faults_confirmed += 1
            logger.info("fault_confirmed", extra=extra)
        else:
            self._stats.faults_unexpected += 1
            logger.warning("fault_mismatch", extra=extra)
