"""Cursor persistence and the at-least-once dedup guard."""

from __future__ import annotations

from collections import deque
from typing import Protocol, runtime_checkable

__all__ = ["CursorStore", "MemoryCursorStore", "ResumeState"]


@runtime_checkable
class CursorStore(Protocol):
    """A hook for loading and persisting the resume cursor across sessions.

    Implement this to make at-least-once delivery survive process restarts:
    :meth:`load` is called once at the start of a run, and :meth:`save` after
    each event is successfully handled. For not-missing-while-briefly-
    disconnected only, :class:`MemoryCursorStore` (the default) is enough.
    """

    async def load(self) -> int:
        """Load the last persisted cursor (0 replays from the retention start)."""
        ...

    async def save(self, cursor: int) -> None:
        """Persist ``cursor``. Called after each successfully-handled event."""
        ...


class MemoryCursorStore:
    """In-memory cursor store seeded from an initial value.

    The default when no persistence hook is configured; does not survive
    process restarts.
    """

    __slots__ = ("_cursor",)

    def __init__(self, initial: int = 0) -> None:
        self._cursor = initial

    async def load(self) -> int:
        return self._cursor

    async def save(self, cursor: int) -> None:
        self._cursor = cursor

    def get(self) -> int:
        """Current cursor value (synchronous convenience for tests/callers)."""
        return self._cursor


class ResumeState:
    """Tracks the resume cursor and a bounded set of recently-seen event ids.

    Implements conformance items 4 and 5: an event is processed at most once,
    and the cursor advances only *after* a successful handle. ``seq`` is a
    **global** monotonic counter, so gaps in the ``seq`` values a connection
    observes are normal (they belong to other subscribers' patterns) — never
    infer loss from a gap.
    """

    __slots__ = ("_cursor", "_seen", "_order", "_capacity")

    def __init__(self, cursor: int, capacity: int) -> None:
        self._cursor = cursor
        self._seen: set[str] = set()
        self._order: deque[str] = deque()
        self._capacity = max(capacity, 1)

    @property
    def cursor(self) -> int:
        """The highest ``seq`` successfully processed — the next ``after_seq``."""
        return self._cursor

    def should_process(self, event_id: str, seq: int) -> bool:
        """Whether an event with this ``id``/``seq`` should be handled.

        Skips duplicates around the replay/live boundary: a ``seq`` at or below
        the cursor, or an ``id`` already processed within the recent window.
        """
        return seq > self._cursor and event_id not in self._seen

    def record(self, event_id: str, seq: int) -> None:
        """Record a successfully-handled event: remember its ``id`` (evicting the
        oldest beyond ``capacity``) and advance the cursor."""
        if event_id not in self._seen:
            self._seen.add(event_id)
            self._order.append(event_id)
            if len(self._order) > self._capacity:
                evicted = self._order.popleft()
                self._seen.discard(evicted)
        if seq > self._cursor:
            self._cursor = seq
