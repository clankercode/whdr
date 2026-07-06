"""Error types for the subscriber client.

:meth:`SubscriberError.is_fatal` distinguishes *fatal* errors (the run loop
stops and raises them) from *transient* errors (the loop reconnects with
backoff). Per the reconnect-and-resume algorithm (SPEC §9.4), an auth failure,
a ``revoked`` close, a handler error, and a cursor-store failure are fatal;
everything else — a dropped socket, a server ``shutdown``, a ``lagged``
eviction — is transient.
"""

from __future__ import annotations

__all__ = [
    "SubscriberError",
    "AuthError",
    "HttpError",
    "RevokedError",
    "HandlerError",
    "CursorStoreError",
    "TransportError",
    "ConnectionClosedError",
]


class SubscriberError(Exception):
    """Base class for all subscriber-client errors."""

    def is_fatal(self) -> bool:
        """Whether the run loop should stop and raise this instead of reconnecting."""
        return False


class AuthError(SubscriberError):
    """The WebSocket upgrade was rejected with HTTP 401. **Fatal.**"""

    def __init__(self) -> None:
        super().__init__("authentication failed (HTTP 401): token missing, wrong, or revoked")

    def is_fatal(self) -> bool:
        return True


class HttpError(SubscriberError):
    """The WebSocket upgrade failed with a non-401 HTTP status. **Transient.**"""

    def __init__(self, status: int) -> None:
        self.status = status
        super().__init__(f"websocket upgrade failed with HTTP {status}")


class RevokedError(SubscriberError):
    """Server sent ``closing`` with reason ``revoked``. **Fatal.**"""

    def __init__(self) -> None:
        super().__init__("connection closed by server: token revoked")

    def is_fatal(self) -> bool:
        return True


class HandlerError(SubscriberError):
    """The application event handler raised. **Fatal.**"""

    def __init__(self, source: BaseException) -> None:
        self.source = source
        super().__init__(f"event handler failed: {source}")

    def is_fatal(self) -> bool:
        return True


class CursorStoreError(SubscriberError):
    """A cursor-persistence hook failed. **Fatal** (a client that cannot persist
    its cursor cannot honour its at-least-once contract)."""

    def __init__(self, source: BaseException) -> None:
        self.source = source
        super().__init__(f"cursor store failed: {source}")

    def is_fatal(self) -> bool:
        return True


class TransportError(SubscriberError):
    """The connection errored at the transport layer. **Transient.**"""


class ConnectionClosedError(SubscriberError):
    """The connection closed (cleanly or with a bare close frame). **Transient.**"""

    def __init__(self) -> None:
        super().__init__("connection closed")
