"""``whdr_sub_kit`` — the async Python client for the **whdr** subscriber plane.

whdr fans provider-webhook events out to token-authenticated WebSocket
subscribers. With durable delivery enabled on the server, a subscriber can
**resume from a cursor** and replay events it missed while offline or after a
slow-consumer drop — at-least-once, de-duplicated by event ``id``.

This library mirrors the behaviour of the reference Rust ``whdr-sub-kit`` and
implements the 10-point *Subscriber wire protocol v2* conformance checklist.

Quick start::

    import asyncio
    from whdr_sub_kit import Subscriber

    async def main() -> None:
        sub = Subscriber(
            "ws://127.0.0.1:8788/subscribe",
            "tok_your_token",
            patterns=["github.>"],
            cursor=0,  # 0 = replay from the start of retention
        )
        async for event in sub.events():
            print(event.channel, event.seq, len(event.payload()))

    asyncio.run(main())
"""

from __future__ import annotations

from ._backoff import Backoff, BackoffPolicy
from ._connection import Connection
from ._cursor import CursorStore, MemoryCursorStore, ResumeState
from ._errors import (
    AuthError,
    ConnectionClosedError,
    CursorStoreError,
    HandlerError,
    HttpError,
    RevokedError,
    SubscriberError,
    TransportError,
)
from ._frames import (
    Closing,
    DeliveredEvent,
    ErrorFrame,
    Lagged,
    Ok,
    Pong,
    Replayed,
    ReplayGap,
    ServerFrame,
    Welcome,
    parse_frame,
    subscribe_message,
)
from ._subscriber import EventCallback, Handler, SignalCallback, Subscriber

__version__ = "0.1.0"

__all__ = [
    # Core API
    "Subscriber",
    "Handler",
    "EventCallback",
    "SignalCallback",
    "DeliveredEvent",
    # Cursor / dedup
    "CursorStore",
    "MemoryCursorStore",
    "ResumeState",
    # Backoff
    "BackoffPolicy",
    "Backoff",
    # Low-level connection + frames
    "Connection",
    "parse_frame",
    "subscribe_message",
    "ServerFrame",
    "Welcome",
    "Ok",
    "ErrorFrame",
    "Replayed",
    "ReplayGap",
    "Lagged",
    "Pong",
    "Closing",
    # Errors
    "SubscriberError",
    "AuthError",
    "HttpError",
    "RevokedError",
    "HandlerError",
    "CursorStoreError",
    "TransportError",
    "ConnectionClosedError",
    "__version__",
]
