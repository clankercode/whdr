"""Integration tests against a real ``whdr-server`` binary.

Skipped when the prebuilt binaries are absent. When present, these drive the
actual client library against the daemon over a live WebSocket, covering: live
subscribe, resume-after-disconnect exactly-once replay, the durability-disabled
path, and a bad-token fatal auth failure.
"""

from __future__ import annotations

import asyncio

import pytest
from _server import BINARIES_AVAILABLE, SKIP_REASON, Server

from whdr_sub_kit import (
    AuthError,
    BackoffPolicy,
    DeliveredEvent,
    Handler,
    MemoryCursorStore,
    Subscriber,
)

pytestmark = pytest.mark.skipif(not BINARIES_AVAILABLE, reason=SKIP_REASON)

FAST = BackoffPolicy(initial=0.02, max=0.2, multiplier=2.0, jitter=0.0)
CHANNEL_PATTERN = ["alpha.>"]


class Collector(Handler):
    """Records events (payload text) and fires an event-loop signal per seq."""

    def __init__(self) -> None:
        self.events: list[DeliveredEvent] = []
        self._on_seq: dict[int, asyncio.Event] = {}
        self.replay_unavailable: list[str] = []
        self.replay_gaps: list[tuple[int, int]] = []

    async def on_event(self, event: DeliveredEvent) -> None:
        self.events.append(event)
        self._on_seq.setdefault(event.seq, asyncio.Event()).set()

    async def on_replay_unavailable(self, msg: str) -> None:
        self.replay_unavailable.append(msg)

    async def on_replay_gap(self, from_seq: int, earliest_seq: int) -> None:
        self.replay_gaps.append((from_seq, earliest_seq))

    async def wait_for_seq(self, seq: int, timeout: float = 10.0) -> None:
        ev = self._on_seq.setdefault(seq, asyncio.Event())
        await asyncio.wait_for(ev.wait(), timeout)

    def payloads(self) -> list[bytes]:
        return [e.payload() for e in self.events]


async def _run_task(sub: Subscriber, handler: Handler) -> asyncio.Task[None]:
    return asyncio.create_task(sub.run(handler))


async def _cancel(task: asyncio.Task[None]) -> None:
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


@pytest.mark.asyncio
async def test_live_subscribe_receives_events() -> None:
    """A connected subscriber receives a freshly-emitted event live."""
    async with Server(delivery=True) as server:
        token = await server.token_add("live")
        sub = Subscriber(server.sub_url, token, patterns=CHANNEL_PATTERN, cursor=0, backoff=FAST)
        collector = Collector()
        task = await _run_task(sub, collector)
        try:
            # Let the subscription establish, then emit.
            await asyncio.sleep(0.3)
            assert await server.emit(b"hello-live") == 200
            await collector.wait_for_seq(1)
            assert collector.payloads() == [b"hello-live"]
        finally:
            await _cancel(task)


@pytest.mark.asyncio
async def test_resume_after_disconnect_replays_missed_exactly_once() -> None:
    """Events missed while disconnected are replayed exactly once at the handler
    on reconnect from the persisted cursor."""
    async with Server(delivery=True) as server:
        token = await server.token_add("resumer")
        store = MemoryCursorStore(0)  # shared across the two sessions = persistence

        # Session 1: connect, receive event 1, then disconnect.
        sub1 = Subscriber(server.sub_url, token, patterns=CHANNEL_PATTERN, cursor_store=store, backoff=FAST)
        c1 = Collector()
        task1 = await _run_task(sub1, c1)
        await asyncio.sleep(0.3)
        assert await server.emit(b"one") == 200
        await c1.wait_for_seq(1)
        await _cancel(task1)
        assert [e.seq for e in c1.events] == [1]
        assert store.get() == 1  # cursor persisted at 1

        # Events 2 and 3 are emitted while NOBODY is connected.
        assert await server.emit(b"two") == 200
        assert await server.emit(b"three") == 200

        # Session 2: resume from the stored cursor (1). Replays 2 and 3 only.
        sub2 = Subscriber(server.sub_url, token, patterns=CHANNEL_PATTERN, cursor_store=store, backoff=FAST)
        c2 = Collector()
        task2 = await _run_task(sub2, c2)
        try:
            await c2.wait_for_seq(3)
            # Exactly-once: event 1 is NOT redelivered; 2 and 3 each appear once.
            seqs = [e.seq for e in c2.events]
            assert seqs == [2, 3], f"expected exactly-once replay of 2,3; got {seqs}"
            assert c2.payloads() == [b"two", b"three"]
            assert store.get() == 3
        finally:
            await _cancel(task2)


@pytest.mark.asyncio
async def test_durability_disabled_refuses_replay_but_live_continues() -> None:
    """With delivery off, a replay request is refused (error op replay) and the
    subscriber keeps working live-only."""
    async with Server(delivery=False) as server:
        token = await server.token_add("livey")
        sub = Subscriber(server.sub_url, token, patterns=CHANNEL_PATTERN, cursor=0, backoff=FAST)
        collector = Collector()
        task = await _run_task(sub, collector)
        try:
            await asyncio.sleep(0.4)  # allow ok + error(op=replay) to arrive
            assert collector.replay_unavailable, "expected a replay-unavailable signal"
            assert "not enabled" in collector.replay_unavailable[0]
            # Live delivery still works.
            assert await server.emit(b"still-live") == 200
            await collector.wait_for_seq(1)
            assert collector.payloads() == [b"still-live"]
        finally:
            await _cancel(task)
        # No delivery store file is created when disabled.
        assert not server.store_path().exists()


@pytest.mark.asyncio
async def test_bad_token_is_fatal() -> None:
    """An unknown token fails the upgrade with 401 -> fatal AuthError."""
    async with Server(delivery=True) as server:
        sub = Subscriber(
            server.sub_url, "tok_not_a_real_token", patterns=CHANNEL_PATTERN, cursor=0, backoff=FAST
        )
        with pytest.raises(AuthError):
            await asyncio.wait_for(sub.run(Collector()), 10.0)


@pytest.mark.asyncio
async def test_store_file_is_0600_when_enabled() -> None:
    """The at-rest delivery store is mode 0600 ([D-dursec])."""
    async with Server(delivery=True) as server:
        token = await server.token_add("perms")
        assert await server.emit(b"persist-me") == 200
        # The event is persisted on fanout; connect once to be sure the store
        # exists, then check its mode.
        sub = Subscriber(server.sub_url, token, patterns=CHANNEL_PATTERN, cursor=0, backoff=FAST)
        collector = Collector()
        task = await _run_task(sub, collector)
        try:
            await collector.wait_for_seq(1)
        finally:
            await _cancel(task)
        assert server.store_mode() == 0o600
