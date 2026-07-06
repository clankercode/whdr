"""Protocol-behaviour tests against an in-process ``websockets`` mock server.

These exercise the reconnect-and-resume algorithm end-to-end over a real
WebSocket, including the ok-before-replay_gap ordering subtlety.
"""

from __future__ import annotations

import asyncio

import pytest
from conftest import mock_server, read_subscribe, send_json, welcome
from websockets.asyncio.server import ServerConnection

from whdr_sub_kit import (
    AuthError,
    BackoffPolicy,
    DeliveredEvent,
    Handler,
    HandlerError,
    RevokedError,
    Subscriber,
)

FAST = BackoffPolicy(initial=0.001, max=0.01, multiplier=2.0, jitter=0.0)


def event(seq: int, id_: str, channel: str = "dev.x", payload_b64: str = "AA==") -> dict:
    return {
        "type": "event",
        "id": id_,
        "seq": seq,
        "ts_ms": 1000 + seq,
        "channel": channel,
        "payload_b64": payload_b64,
    }


async def _collect(sub: Subscriber, n: int, *, timeout: float = 5.0) -> list[DeliveredEvent]:
    """Collect ``n`` events from ``sub.events()`` then stop iterating."""
    out: list[DeliveredEvent] = []

    async def drain() -> None:
        async for ev in sub.events():
            out.append(ev)
            if len(out) >= n:
                return

    await asyncio.wait_for(drain(), timeout)
    return out


# --------------------------------------------------------------- replay + live


@pytest.mark.asyncio
async def test_replay_then_live_stream() -> None:
    """subscribe(after_seq=0) -> ok, event1, event2, replayed, then live event3."""

    async def handler(ws: ServerConnection) -> None:
        await welcome(ws)
        sub = await read_subscribe(ws)
        assert sub["replay"] == {"after_seq": 0}  # item 3
        await send_json(ws, {"type": "ok", "op": "subscribe"})
        await send_json(ws, event(1, "a"))
        await send_json(ws, event(2, "b"))
        await send_json(ws, {"type": "replayed", "through_seq": 2})
        await send_json(ws, event(3, "c"))  # live
        await asyncio.sleep(0.2)  # keep the socket open until the client is done

    async with mock_server(handler) as url:
        sub = Subscriber(url, "tok", patterns=["dev.>"], cursor=0, backoff=FAST)
        events = await _collect(sub, 3)

    assert [e.seq for e in events] == [1, 2, 3]
    assert [e.id for e in events] == ["a", "b", "c"]


@pytest.mark.asyncio
async def test_ok_before_replay_gap_ordering() -> None:
    """Subtlety #1/#2: `ok` first, THEN replay_gap; earliest_seq itself is
    delivered. The client must be order-agnostic and surface the gap."""
    gaps: list[tuple[int, int]] = []

    async def handler(ws: ServerConnection) -> None:
        await welcome(ws)
        await read_subscribe(ws)
        await send_json(ws, {"type": "ok", "op": "subscribe"})
        # ok arrives BEFORE the replay_gap — client must not assume gap-first.
        await send_json(ws, {"type": "replay_gap", "from_seq": 0, "earliest_seq": 5})
        await send_json(ws, event(5, "e5"))  # earliest_seq IS delivered
        await send_json(ws, event(6, "e6"))
        await send_json(ws, {"type": "replayed", "through_seq": 6})
        await asyncio.sleep(0.2)

    async with mock_server(handler) as url:
        sub = Subscriber(
            url,
            "tok",
            patterns=["dev.>"],
            cursor=0,
            backoff=FAST,
            on_replay_gap=lambda f, e: gaps.append((f, e)),
        )
        events = await _collect(sub, 2)

    assert gaps == [(0, 5)]  # item 7
    assert [e.seq for e in events] == [5, 6]  # earliest_seq (5) delivered


@pytest.mark.asyncio
async def test_dedup_across_replay_live_boundary() -> None:
    """Item 4: a duplicate (same id/seq) at the replay/live boundary is dropped."""

    async def handler(ws: ServerConnection) -> None:
        await welcome(ws)
        await read_subscribe(ws)
        await send_json(ws, {"type": "ok", "op": "subscribe"})
        await send_json(ws, event(1, "dup"))  # replayed
        await send_json(ws, {"type": "replayed", "through_seq": 1})
        await send_json(ws, event(1, "dup"))  # live duplicate of the same event
        await send_json(ws, event(2, "new"))  # genuinely new
        await asyncio.sleep(0.2)

    async with mock_server(handler) as url:
        sub = Subscriber(url, "tok", patterns=["dev.>"], cursor=0, backoff=FAST)
        events = await _collect(sub, 2)

    assert [(e.seq, e.id) for e in events] == [(1, "dup"), (2, "new")]


@pytest.mark.asyncio
async def test_unknown_frame_type_ignored_over_the_wire() -> None:
    """Item 10: unknown frame types on the wire are skipped, not fatal."""

    async def handler(ws: ServerConnection) -> None:
        await welcome(ws)
        await read_subscribe(ws)
        await send_json(ws, {"type": "ok", "op": "subscribe"})
        await send_json(ws, {"type": "mystery_frame", "whatever": 1})
        await send_json(ws, event(1, "a"))
        await asyncio.sleep(0.2)

    async with mock_server(handler) as url:
        sub = Subscriber(url, "tok", patterns=["dev.>"], cursor=0, backoff=FAST)
        events = await _collect(sub, 1)

    assert [e.seq for e in events] == [1]


# ----------------------------------------------------------- lagged / reconnect


@pytest.mark.asyncio
async def test_lagged_reconnects_and_resumes_from_cursor() -> None:
    """Item 6: a `lagged` frame triggers reconnect; the resume subscribe carries
    replay.after_seq = cursor (the highest processed seq)."""
    subscribes: list[dict] = []
    lagged_seen: list[int] = []
    second_conn = asyncio.Event()

    async def handler(ws: ServerConnection) -> None:
        await welcome(ws)
        sub = await read_subscribe(ws)
        subscribes.append(sub)
        await send_json(ws, {"type": "ok", "op": "subscribe"})
        if len(subscribes) == 1:
            await send_json(ws, event(1, "a"))
            await send_json(ws, event(2, "b"))
            await asyncio.sleep(0.05)  # let the client process before lagging
            await send_json(ws, {"type": "lagged", "dropped": 3})
            await asyncio.sleep(0.2)
        else:
            second_conn.set()
            await send_json(ws, event(3, "c"))
            await asyncio.sleep(0.2)

    async with mock_server(handler) as url:
        sub = Subscriber(
            url,
            "tok",
            patterns=["dev.>"],
            cursor=0,
            backoff=FAST,
            on_lagged=lambda d: lagged_seen.append(d),
        )
        events = await _collect(sub, 3)
        await asyncio.wait_for(second_conn.wait(), 5.0)

    assert lagged_seen == [3]
    # First subscribe resumes from 0; second (post-lag) resumes from cursor=2.
    assert subscribes[0]["replay"] == {"after_seq": 0}
    assert subscribes[1]["replay"] == {"after_seq": 2}
    assert [e.seq for e in events] == [1, 2, 3]


@pytest.mark.asyncio
async def test_shutdown_closing_reconnects() -> None:
    """Item 8: `closing` with reason `shutdown` reconnects with backoff."""
    subscribes: list[dict] = []

    async def handler(ws: ServerConnection) -> None:
        await welcome(ws)
        subscribes.append(await read_subscribe(ws))
        await send_json(ws, {"type": "ok", "op": "subscribe"})
        if len(subscribes) == 1:
            await send_json(ws, {"type": "closing", "reason": "shutdown"})
            await asyncio.sleep(0.2)
        else:
            await send_json(ws, event(1, "a"))
            await asyncio.sleep(0.2)

    async with mock_server(handler) as url:
        sub = Subscriber(url, "tok", patterns=["dev.>"], cursor=0, backoff=FAST)
        events = await _collect(sub, 1)

    assert len(subscribes) >= 2  # reconnected after shutdown
    assert [e.seq for e in events] == [1]


# --------------------------------------------------------------- fatal signals


@pytest.mark.asyncio
async def test_revoked_closing_is_fatal() -> None:
    """Item 8: `closing` reason `revoked` raises RevokedError and stops."""

    async def handler(ws: ServerConnection) -> None:
        await welcome(ws)
        await read_subscribe(ws)
        await send_json(ws, {"type": "ok", "op": "subscribe"})
        await send_json(ws, {"type": "closing", "reason": "revoked"})
        await asyncio.sleep(0.2)

    async with mock_server(handler) as url:
        sub = Subscriber(url, "tok", patterns=["dev.>"], cursor=0, backoff=FAST)
        with pytest.raises(RevokedError):
            async for _ in sub.events():
                pass


@pytest.mark.asyncio
async def test_bad_token_is_fatal_auth_error() -> None:
    """Item 1: a 401 upgrade rejection is a fatal AuthError."""

    async def handler(ws: ServerConnection) -> None:  # pragma: no cover - never reached
        await welcome(ws)

    async with mock_server(handler, expected_token="good") as url:
        sub = Subscriber(url, "bad", patterns=["dev.>"], cursor=0, backoff=FAST)
        with pytest.raises(AuthError):
            async for _ in sub.events():
                pass


# ---------------------------------------------------------------- run(handler)


@pytest.mark.asyncio
async def test_run_handler_advances_cursor_and_handles_signals() -> None:
    """run() drives a Handler; cursor advances after on_event; signal hooks fire."""
    processed: list[int] = []
    replayed_at: list[int] = []
    unavailable: list[str] = []

    class Rec(Handler):
        async def on_event(self, ev: DeliveredEvent) -> None:
            processed.append(ev.seq)

        async def on_replayed(self, through_seq: int) -> None:
            replayed_at.append(through_seq)

        async def on_replay_unavailable(self, msg: str) -> None:
            unavailable.append(msg)

    async def handler(ws: ServerConnection) -> None:
        await welcome(ws)
        await read_subscribe(ws)
        await send_json(ws, {"type": "ok", "op": "subscribe"})
        await send_json(ws, {"type": "error", "op": "replay", "msg": "durable delivery is not enabled"})
        await send_json(ws, event(1, "a"))
        await send_json(ws, {"type": "replayed", "through_seq": 1})
        await send_json(ws, event(2, "b"))
        await asyncio.sleep(0.3)

    async with mock_server(handler) as url:
        sub = Subscriber(url, "tok", patterns=["dev.>"], cursor=0, backoff=FAST)
        task = asyncio.create_task(sub.run(Rec()))
        # Wait until both events are processed, then cancel the forever-loop.
        for _ in range(100):
            if processed == [1, 2]:
                break
            await asyncio.sleep(0.02)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    assert processed == [1, 2]
    assert replayed_at == [1]
    assert unavailable == ["durable delivery is not enabled"]


@pytest.mark.asyncio
async def test_run_handler_error_is_fatal() -> None:
    """A handler raising stops run() with a fatal HandlerError."""

    class Boom(Handler):
        async def on_event(self, ev: DeliveredEvent) -> None:
            raise ValueError("boom")

    async def handler(ws: ServerConnection) -> None:
        await welcome(ws)
        await read_subscribe(ws)
        await send_json(ws, {"type": "ok", "op": "subscribe"})
        await send_json(ws, event(1, "a"))
        await asyncio.sleep(0.3)

    async with mock_server(handler) as url:
        sub = Subscriber(url, "tok", patterns=["dev.>"], cursor=0, backoff=FAST)
        with pytest.raises(HandlerError) as excinfo:
            await asyncio.wait_for(sub.run(Boom()), 5.0)
    assert isinstance(excinfo.value.source, ValueError)


@pytest.mark.asyncio
async def test_run_accepts_bare_async_callable() -> None:
    """run() also accepts a plain async callable as the event handler."""
    seen: list[int] = []

    async def on_event(ev: DeliveredEvent) -> None:
        seen.append(ev.seq)

    async def handler(ws: ServerConnection) -> None:
        await welcome(ws)
        await read_subscribe(ws)
        await send_json(ws, {"type": "ok", "op": "subscribe"})
        await send_json(ws, event(1, "a"))
        await asyncio.sleep(0.3)

    async with mock_server(handler) as url:
        sub = Subscriber(url, "tok", patterns=["dev.>"], cursor=0, backoff=FAST)
        task = asyncio.create_task(sub.run(on_event))
        for _ in range(100):
            if seen == [1]:
                break
            await asyncio.sleep(0.02)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    assert seen == [1]


@pytest.mark.asyncio
async def test_clean_cancellation_of_events_iterator() -> None:
    """Cancelling a task iterating events() propagates CancelledError cleanly."""

    async def handler(ws: ServerConnection) -> None:
        await welcome(ws)
        await read_subscribe(ws)
        await send_json(ws, {"type": "ok", "op": "subscribe"})
        await send_json(ws, event(1, "a"))
        await asyncio.sleep(5.0)  # then idle; the client is blocked in recv()

    seen: list[int] = []

    async def loop(sub: Subscriber) -> None:
        async for ev in sub.events():
            seen.append(ev.seq)

    async with mock_server(handler) as url:
        sub = Subscriber(url, "tok", patterns=["dev.>"], cursor=0, backoff=FAST)
        task = asyncio.create_task(loop(sub))
        for _ in range(100):
            if seen:
                break
            await asyncio.sleep(0.02)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    assert seen == [1]
