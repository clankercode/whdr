"""The :class:`Subscriber` — the batteries-included reconnect-and-resume client.

Implements the appendix §7 algorithm: auth -> welcome -> subscribe with
``replay.after_seq = cursor`` -> dedup by ``id`` / ``seq`` -> advance the cursor
after each successful handle -> recover from ``lagged`` / disconnects by
reconnecting from the cursor -> surface ``replay_gap`` -> treat ``revoked`` as
fatal and ``shutdown`` as a backoff reconnect.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
from collections.abc import AsyncIterator, Awaitable, Callable, Iterable
from typing import Any, Union

from ._backoff import Backoff, BackoffPolicy
from ._connection import Connection
from ._cursor import CursorStore, MemoryCursorStore, ResumeState
from ._errors import (
    CursorStoreError,
    HandlerError,
    RevokedError,
    SubscriberError,
)
from ._frames import (
    Closing,
    DeliveredEvent,
    ErrorFrame,
    Lagged,
    Replayed,
    ReplayGap,
    ServerFrame,
)

logger = logging.getLogger("whdr_sub_kit")

__all__ = ["Subscriber", "Handler", "EventCallback", "SignalCallback"]

#: An async or sync callable handling a single delivered event.
EventCallback = Callable[[DeliveredEvent], Union[Awaitable[None], None]]
#: An async or sync callable for a signal frame (replayed / lagged / ...).
SignalCallback = Callable[..., Union[Awaitable[None], None]]


class Handler:
    """Base class for the :meth:`Subscriber.run` handler.

    Override :meth:`on_event` (required); the signal hooks default to logging
    (for ``replay_gap``) or nothing. Raising from any hook is **fatal**: the run
    loop stops and raises :class:`~whdr_sub_kit.HandlerError`. The cursor is
    advanced (and persisted) only *after* :meth:`on_event` returns, giving
    at-least-once delivery.
    """

    async def on_event(self, event: DeliveredEvent) -> None:
        """Handle a delivered event (de-duplicated: called at most once)."""
        raise NotImplementedError

    async def on_replayed(self, through_seq: int) -> None:
        """A replay window finished; live frames follow. Default: no-op."""

    async def on_replay_gap(self, from_seq: int, earliest_seq: int) -> None:
        """Explicit data-loss signal: events in ``(from_seq, earliest_seq)`` were
        pruned before this subscriber resumed. Default: logs a warning."""
        logger.warning(
            "replay_gap: events (%d, %d) were pruned before this subscriber resumed",
            from_seq,
            earliest_seq,
        )

    async def on_lagged(self, dropped: int) -> None:
        """The server evicted ``dropped`` events; the kit reconnects and replays
        from the cursor to recover. Default: no-op."""

    async def on_replay_unavailable(self, msg: str) -> None:
        """A ``replay`` request was refused because durable delivery is disabled;
        live delivery still works. Default: no-op."""


async def _maybe_await(value: Awaitable[None] | None) -> None:
    if inspect.isawaitable(value):
        await value


class _Signals:
    """Signal-frame sinks built from the :class:`Subscriber` constructor
    callbacks, with the reference defaults (``replay_gap`` logs; others no-op).

    Shares its method names with :class:`_HandlerHooks` so both satisfy the
    control-frame dispatcher's interface.
    """

    __slots__ = ("_replayed", "_replay_gap", "_lagged", "_replay_unavailable")

    def __init__(
        self,
        on_replayed: SignalCallback | None,
        on_replay_gap: SignalCallback | None,
        on_lagged: SignalCallback | None,
        on_replay_unavailable: SignalCallback | None,
    ) -> None:
        self._replayed = on_replayed
        self._replay_gap = on_replay_gap
        self._lagged = on_lagged
        self._replay_unavailable = on_replay_unavailable

    async def replayed(self, through_seq: int) -> None:
        if self._replayed is not None:
            await _maybe_await(self._replayed(through_seq))

    async def replay_gap(self, from_seq: int, earliest_seq: int) -> None:
        if self._replay_gap is not None:
            await _maybe_await(self._replay_gap(from_seq, earliest_seq))
        else:
            logger.warning(
                "replay_gap: events (%d, %d) were pruned before this subscriber resumed",
                from_seq,
                earliest_seq,
            )

    async def lagged(self, dropped: int) -> None:
        if self._lagged is not None:
            await _maybe_await(self._lagged(dropped))

    async def replay_unavailable(self, msg: str) -> None:
        if self._replay_unavailable is not None:
            await _maybe_await(self._replay_unavailable(msg))


class _HandlerHooks:
    """Adapts a :class:`Handler` to the control-frame dispatcher interface."""

    __slots__ = ("_handler",)

    def __init__(self, handler: Handler) -> None:
        self._handler = handler

    async def replayed(self, through_seq: int) -> None:
        await self._handler.on_replayed(through_seq)

    async def replay_gap(self, from_seq: int, earliest_seq: int) -> None:
        await self._handler.on_replay_gap(from_seq, earliest_seq)

    async def lagged(self, dropped: int) -> None:
        await self._handler.on_lagged(dropped)

    async def replay_unavailable(self, msg: str) -> None:
        await self._handler.on_replay_unavailable(msg)


class _FnHandler(Handler):
    """Wraps a bare event callable as a :class:`Handler`, routing signal frames
    to the subscriber's constructor callbacks."""

    def __init__(self, fn: EventCallback, signals: _Signals) -> None:
        self._fn = fn
        self._signals = signals

    async def on_event(self, event: DeliveredEvent) -> None:
        await _maybe_await(self._fn(event))

    async def on_replayed(self, through_seq: int) -> None:
        await self._signals.replayed(through_seq)

    async def on_replay_gap(self, from_seq: int, earliest_seq: int) -> None:
        await self._signals.replay_gap(from_seq, earliest_seq)

    async def on_lagged(self, dropped: int) -> None:
        await self._signals.lagged(dropped)

    async def on_replay_unavailable(self, msg: str) -> None:
        await self._signals.replay_unavailable(msg)


# Interface shared by _Signals and _HandlerHooks.
_ControlHooks = Union[_Signals, _HandlerHooks]


async def _dispatch_control(frame: ServerFrame, hooks: _ControlHooks) -> str | None:
    """Handle a non-event frame.

    Returns ``"reconnect"`` when the session should end and resume from the
    cursor (``lagged``, or a ``closing`` that is not ``revoked``); ``None`` to
    keep reading. Raises :class:`RevokedError` for a ``revoked`` close.
    """
    if isinstance(frame, Replayed):
        await hooks.replayed(frame.through_seq)
        return None
    if isinstance(frame, ReplayGap):
        await hooks.replay_gap(frame.from_seq, frame.earliest_seq)
        return None
    if isinstance(frame, Lagged):
        await hooks.lagged(frame.dropped)
        return "reconnect"
    if isinstance(frame, ErrorFrame):
        if frame.op == "replay":
            logger.warning("replay refused (durability disabled): %s", frame.msg)
            await hooks.replay_unavailable(frame.msg)
        else:
            logger.warning("server error frame op=%s msg=%s", frame.op, frame.msg)
        return None
    if isinstance(frame, Closing):
        if frame.reason == "revoked":
            raise RevokedError()
        # "shutdown" (or any other reason): reconnect with backoff.
        return "reconnect"
    # Welcome (unexpected repeat), Ok, Pong: nothing to do.
    return None


class Subscriber:
    """A configured, self-driving whdr subscriber.

    Two ways to consume events, both implementing the full reconnect-and-resume
    algorithm (dedup, cursor advance, backoff, ``lagged``/``shutdown`` recovery,
    ``revoked`` = fatal):

    - ``async for event in sub.events(): ...`` — the typed event stream. The
      cursor advances (and is persisted) once your loop body finishes an event.
    - ``await sub.run(handler)`` — hand a :class:`Handler` (or a bare async
      callable) and the kit calls it per event, advancing the cursor after each
      successful call.

    Signal frames (``replayed``, ``replay_gap``, ``lagged``,
    ``replay_unavailable``) are surfaced via the optional constructor callbacks
    for :meth:`events`, or via the :class:`Handler`'s hooks for :meth:`run`.
    """

    def __init__(
        self,
        url: str,
        token: str,
        *,
        patterns: Iterable[str] | str,
        cursor: int = 0,
        cursor_store: CursorStore | None = None,
        backoff: BackoffPolicy | None = None,
        dedup_capacity: int = 8192,
        on_replayed: SignalCallback | None = None,
        on_replay_gap: SignalCallback | None = None,
        on_lagged: SignalCallback | None = None,
        on_replay_unavailable: SignalCallback | None = None,
        open_timeout: float | None = 10.0,
        connect_kwargs: dict[str, Any] | None = None,
    ) -> None:
        self._url = url
        self._token = token
        self._patterns = [patterns] if isinstance(patterns, str) else list(patterns)
        self._store: CursorStore = cursor_store or MemoryCursorStore(cursor)
        self._backoff = backoff or BackoffPolicy()
        self._dedup_capacity = max(dedup_capacity, 1)
        self._signals = _Signals(
            on_replayed, on_replay_gap, on_lagged, on_replay_unavailable
        )
        opts: dict[str, Any] = dict(connect_kwargs or {})
        if open_timeout is not None:
            opts.setdefault("open_timeout", open_timeout)
        self._opts = opts

    # -- construction helpers -------------------------------------------------

    def _connection(self) -> Connection:
        return Connection(self._url, self._token, **self._opts)

    def _normalize_handler(self, handler: Handler | EventCallback) -> Handler:
        if isinstance(handler, Handler):
            return handler
        if callable(handler):
            return _FnHandler(handler, self._signals)
        raise TypeError("handler must be a Handler or an event callable")

    async def _load_cursor(self) -> int:
        try:
            return await self._store.load()
        except Exception as err:  # noqa: BLE001 — cursor store is a user hook
            raise CursorStoreError(err) from err

    async def _save_cursor(self, cursor: int) -> None:
        try:
            await self._store.save(cursor)
        except Exception as err:  # noqa: BLE001 — cursor store is a user hook
            raise CursorStoreError(err) from err

    # -- run(handler) ---------------------------------------------------------

    async def run(self, handler: Handler | EventCallback) -> None:
        """Run the full reconnect-and-resume loop, driving ``handler``.

        Loops forever, reconnecting with exponential backoff after a transient
        failure (dropped socket, server ``shutdown``, ``lagged`` eviction).
        Returns only if the inner loop is broken by cancellation; raises on a
        **fatal** error (revoked/absent token, handler failure, cursor-store
        failure).
        """
        h = self._normalize_handler(handler)
        hooks = _HandlerHooks(h)
        resume = ResumeState(await self._load_cursor(), self._dedup_capacity)
        backoff = self._backoff.start()
        while True:
            try:
                await self._run_session(h, hooks, resume, backoff)
                logger.info("subscriber session ended; reconnecting")
            except SubscriberError as err:
                if err.is_fatal():
                    raise
                logger.warning("subscriber session error; reconnecting: %s", err)
            await asyncio.sleep(backoff.next_delay())

    async def _run_session(
        self,
        handler: Handler,
        hooks: _HandlerHooks,
        resume: ResumeState,
        backoff: Backoff,
    ) -> None:
        async with self._connection() as conn:
            # Connected: reset backoff so a later drop reconnects fast.
            backoff.reset()
            await conn.subscribe(self._patterns, resume.cursor)
            while True:
                frame = await conn.recv()
                if isinstance(frame, DeliveredEvent):
                    if resume.should_process(frame.id, frame.seq):
                        try:
                            await handler.on_event(frame)
                        except asyncio.CancelledError:
                            raise
                        except Exception as err:  # noqa: BLE001
                            raise HandlerError(err) from err
                        resume.record(frame.id, frame.seq)
                        await self._save_cursor(resume.cursor)
                else:
                    if await _dispatch_control(frame, hooks) == "reconnect":
                        return

    # -- events() iterator ----------------------------------------------------

    async def events(self) -> AsyncIterator[DeliveredEvent]:
        """Yield de-duplicated events, reconnecting and resuming forever.

        The cursor advances (and is persisted) after your loop body finishes
        processing each yielded event, giving at-least-once delivery. Signal
        frames go to the constructor callbacks (``replay_gap`` logs by default).
        A fatal error (revoked token, cursor-store failure) propagates out of
        the iterator; transient failures reconnect with backoff.
        """
        resume = ResumeState(await self._load_cursor(), self._dedup_capacity)
        backoff = self._backoff.start()
        hooks = self._signals
        while True:
            try:
                async with self._connection() as conn:
                    backoff.reset()
                    await conn.subscribe(self._patterns, resume.cursor)
                    reconnect = False
                    while not reconnect:
                        frame = await conn.recv()
                        if isinstance(frame, DeliveredEvent):
                            if resume.should_process(frame.id, frame.seq):
                                yield frame
                                resume.record(frame.id, frame.seq)
                                await self._save_cursor(resume.cursor)
                        elif await _dispatch_control(frame, hooks) == "reconnect":
                            reconnect = True
            except SubscriberError as err:
                if err.is_fatal():
                    raise
                logger.warning("subscriber session error; reconnecting: %s", err)
            await asyncio.sleep(backoff.next_delay())
