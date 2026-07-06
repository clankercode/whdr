"""The typed WebSocket connection: authenticated upgrade, welcome handshake,
and a frame-by-frame typed stream.

WebSocket-level ping frames are answered automatically by the ``websockets``
client keepalive (conformance item 9), so nothing here needs to do it.
"""

from __future__ import annotations

from types import TracebackType
from typing import Any

from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import ConnectionClosed, InvalidStatus, WebSocketException

from ._errors import AuthError, ConnectionClosedError, HttpError, TransportError
from ._frames import ServerFrame, Welcome, parse_frame, subscribe_message

__all__ = ["Connection"]


class Connection:
    """An authenticated subscriber connection.

    Use as an async context manager; on entry it connects, authenticates with
    ``Authorization: Bearer <token>`` (conformance item 1), and consumes the
    ``welcome`` frame (conformance item 2). :meth:`recv` then yields typed
    server frames, skipping unrecognised ones (conformance item 10).
    """

    __slots__ = ("_url", "_token", "_opts", "_cm", "_ws", "name")

    def __init__(self, url: str, token: str, **opts: Any) -> None:
        self._url = url
        self._token = token
        self._opts = opts
        self._cm: connect | None = None
        self._ws: ClientConnection | None = None
        self.name: str = ""

    async def __aenter__(self) -> Connection:
        self._cm = connect(
            self._url,
            additional_headers={"Authorization": f"Bearer {self._token}"},
            **self._opts,
        )
        try:
            self._ws = await self._cm.__aenter__()
        except InvalidStatus as err:
            status = err.response.status_code
            raise (AuthError() if status == 401 else HttpError(status)) from err
        except (OSError, WebSocketException) as err:
            raise TransportError(f"connect failed: {err}") from err
        # Read frames until the welcome; anything before it is skipped.
        while True:
            frame = await self.recv()
            if isinstance(frame, Welcome):
                self.name = frame.name
                return self
            # Frame before welcome: ignore and keep reading.

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        if self._cm is not None:
            await self._cm.__aexit__(exc_type, exc, tb)

    async def subscribe(self, patterns: list[str], after_seq: int | None) -> None:
        """Send a ``subscribe``, optionally resuming from ``after_seq``
        (conformance item 3: resume with ``replay.after_seq = cursor``)."""
        assert self._ws is not None
        try:
            await self._ws.send(subscribe_message(patterns, after_seq))
        except ConnectionClosed as err:
            raise ConnectionClosedError() from err
        except (WebSocketException, OSError) as err:
            raise TransportError(f"send failed: {err}") from err

    async def recv(self) -> ServerFrame:
        """Read the next typed server frame, skipping unrecognised frames.

        Raises :class:`ConnectionClosedError` when the peer closes.
        """
        assert self._ws is not None
        while True:
            try:
                raw = await self._ws.recv()
            except ConnectionClosed as err:
                raise ConnectionClosedError() from err
            except (WebSocketException, OSError) as err:
                raise TransportError(f"recv failed: {err}") from err
            frame = parse_frame(raw)
            if frame is not None:
                return frame
            # Unknown frame: keep reading.
