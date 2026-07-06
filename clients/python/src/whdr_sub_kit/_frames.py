"""Frame parsing and the delivered-event view.

Pure, transport-agnostic building blocks. Server frames are decoded from JSON
text into small frozen dataclasses; unknown ``type`` tags and unknown object
fields are ignored (forward compatibility, conformance item 10).
"""

from __future__ import annotations

import base64
import json
from dataclasses import dataclass
from typing import Union

__all__ = [
    "DeliveredEvent",
    "Welcome",
    "Ok",
    "ErrorFrame",
    "Replayed",
    "ReplayGap",
    "Lagged",
    "Pong",
    "Closing",
    "ServerFrame",
    "parse_frame",
    "subscribe_message",
]


@dataclass(frozen=True, slots=True)
class DeliveredEvent:
    """A delivered event decoded from an ``event`` frame.

    ``id`` is stable across live delivery and every replay of the event — dedup
    by ``id``. ``seq`` is the global monotonic cursor key. ``ts_ms`` is the
    server wall-clock at fan-out; it is informational — order by ``seq``.
    """

    id: str
    seq: int
    ts_ms: int
    channel: str
    payload_b64: str

    def payload(self) -> bytes:
        """Standard-base64 decode ``payload_b64`` to raw bytes."""
        return base64.standard_b64decode(self.payload_b64)


@dataclass(frozen=True, slots=True)
class Welcome:
    name: str


@dataclass(frozen=True, slots=True)
class Ok:
    op: str


@dataclass(frozen=True, slots=True)
class ErrorFrame:
    op: str
    msg: str


@dataclass(frozen=True, slots=True)
class Replayed:
    through_seq: int


@dataclass(frozen=True, slots=True)
class ReplayGap:
    from_seq: int
    earliest_seq: int


@dataclass(frozen=True, slots=True)
class Lagged:
    dropped: int


@dataclass(frozen=True, slots=True)
class Pong:
    pass


@dataclass(frozen=True, slots=True)
class Closing:
    #: ``"shutdown"`` or ``"revoked"`` (unknown reasons are preserved as-is).
    reason: str


ServerFrame = Union[
    Welcome,
    Ok,
    ErrorFrame,
    DeliveredEvent,
    Replayed,
    ReplayGap,
    Lagged,
    Pong,
    Closing,
]


def parse_frame(text: str | bytes) -> ServerFrame | None:
    """Parse one JSON text frame into a typed server frame.

    Returns ``None`` for unknown ``type`` tags, malformed JSON, or frames whose
    required fields are missing/mistyped — the caller ignores the frame and
    reads the next one (conformance item 10). Unknown *fields* on a known frame
    are tolerated: only recognised keys are read.
    """
    if isinstance(text, bytes):
        try:
            text = text.decode("utf-8")
        except UnicodeDecodeError:
            return None
    try:
        obj = json.loads(text)
    except (json.JSONDecodeError, ValueError):
        return None
    if not isinstance(obj, dict):
        return None
    kind = obj.get("type")
    if not isinstance(kind, str):
        return None
    try:
        return _decode(kind, obj)
    except (KeyError, TypeError, ValueError):
        # A known type with missing/malformed fields: ignore, keep reading.
        return None


def _decode(kind: str, obj: dict[str, object]) -> ServerFrame | None:
    if kind == "event":
        return DeliveredEvent(
            id=_as_str(obj["id"]),
            seq=_as_int(obj["seq"]),
            ts_ms=_as_int(obj["ts_ms"]),
            channel=_as_str(obj["channel"]),
            payload_b64=_as_str(obj["payload_b64"]),
        )
    if kind == "welcome":
        return Welcome(name=_as_str(obj["name"]))
    if kind == "ok":
        return Ok(op=_as_str(obj["op"]))
    if kind == "error":
        return ErrorFrame(op=_as_str(obj["op"]), msg=_as_str(obj["msg"]))
    if kind == "replayed":
        return Replayed(through_seq=_as_int(obj["through_seq"]))
    if kind == "replay_gap":
        return ReplayGap(
            from_seq=_as_int(obj["from_seq"]),
            earliest_seq=_as_int(obj["earliest_seq"]),
        )
    if kind == "lagged":
        return Lagged(dropped=_as_int(obj["dropped"]))
    if kind == "pong":
        return Pong()
    if kind == "closing":
        return Closing(reason=_as_str(obj["reason"]))
    # Unknown frame type.
    return None


def _as_str(value: object) -> str:
    if not isinstance(value, str):
        raise TypeError(f"expected string, got {type(value).__name__}")
    return value


def _as_int(value: object) -> int:
    # JSON numbers arrive as int; reject bool (a bool is an int subclass).
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"expected integer, got {type(value).__name__}")
    return value


def subscribe_message(patterns: list[str], after_seq: int | None) -> str:
    """Serialise a ``subscribe`` client frame.

    When ``after_seq`` is not ``None`` a ``replay`` cursor is attached
    (conformance item 3: resume with ``replay.after_seq = cursor``); otherwise
    the subscription is live-only (pre-v2 behaviour).
    """
    msg: dict[str, object] = {"type": "subscribe", "patterns": list(patterns)}
    if after_seq is not None:
        msg["replay"] = {"after_seq": after_seq}
    return json.dumps(msg)
