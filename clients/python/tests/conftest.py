"""Shared test helpers: an in-process ``websockets`` mock server."""

from __future__ import annotations

import contextlib
import json
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any

from websockets.asyncio.server import ServerConnection, serve

# A per-connection server coroutine.
Handler = Callable[[ServerConnection], Awaitable[None]]


async def send_json(ws: ServerConnection, obj: dict[str, Any]) -> None:
    await ws.send(json.dumps(obj))


async def welcome(ws: ServerConnection, name: str = "p") -> None:
    await send_json(ws, {"type": "welcome", "name": name})


async def read_subscribe(ws: ServerConnection) -> dict[str, Any]:
    """Read frames until a ``subscribe`` arrives; return it as a dict."""
    async for raw in ws:
        msg = json.loads(raw)
        if msg.get("type") == "subscribe":
            return msg
    raise AssertionError("connection closed before a subscribe frame")


@contextlib.asynccontextmanager
async def mock_server(
    handler: Handler,
    *,
    expected_token: str | None = None,
) -> AsyncIterator[str]:
    """Run ``handler`` as an in-process WebSocket server; yield its ``ws://`` URL.

    When ``expected_token`` is set, upgrades whose ``Authorization`` header is
    not ``Bearer <expected_token>`` are rejected with HTTP 401 before any
    WebSocket frame (used to exercise the auth-failure path).
    """
    process_request = None
    if expected_token is not None:
        want = f"Bearer {expected_token}"

        def process_request(connection: ServerConnection, request: Any) -> Any:
            if request.headers.get("Authorization") != want:
                return connection.respond(401, "unauthorized\n")
            return None

    async with serve(
        handler, "127.0.0.1", 0, process_request=process_request
    ) as server:
        port = server.sockets[0].getsockname()[1]
        yield f"ws://127.0.0.1:{port}/subscribe"
