"""Cursor + dedup guard (ResumeState) and the cursor store."""

from __future__ import annotations

import pytest

from whdr_sub_kit import MemoryCursorStore, ResumeState


def test_skips_seq_at_or_below_cursor() -> None:
    state = ResumeState(0, 16)
    assert state.should_process("a", 1)
    state.record("a", 1)
    assert state.cursor == 1
    # A replayed duplicate at seq 1 is now below the cursor.
    assert not state.should_process("a", 1)
    # A brand-new lower/equal seq is guarded too.
    assert not state.should_process("z", 1)
    # Next higher seq proceeds.
    assert state.should_process("b", 2)


def test_dedups_by_id_across_replay_live_boundary() -> None:
    state = ResumeState(5, 16)
    # Same id delivered once via replay, once live: process exactly once.
    assert state.should_process("id7", 6)
    state.record("id7", 6)
    assert not state.should_process("id7", 6)
    # id7 is in `seen`, so it is skipped even at a higher seq label.
    assert not state.should_process("id7", 8)


def test_cursor_advances_only_via_record() -> None:
    state = ResumeState(10, 16)
    # Merely asking does not move the cursor.
    assert state.should_process("a", 11)
    assert state.cursor == 10
    state.record("a", 11)
    assert state.cursor == 11


def test_bounded_seen_set_evicts_oldest() -> None:
    state = ResumeState(0, 2)
    state.record("1", 1)
    state.record("2", 2)
    state.record("3", 3)  # evicts id "1"
    # id "1" is evicted from the id set, but its seq (1) is below the cursor
    # (now 3), so it is still guarded from reprocessing.
    assert not state.should_process("1", 1)
    # id "2" still remembered by id.
    assert not state.should_process("2", 2)


def test_capacity_floored_at_one() -> None:
    state = ResumeState(0, 0)
    state.record("a", 1)
    state.record("b", 2)  # evicts "a"
    # "a" evicted but guarded by cursor; "b" remembered.
    assert not state.should_process("b", 2)


@pytest.mark.asyncio
async def test_memory_cursor_store_round_trips() -> None:
    store = MemoryCursorStore(42)
    assert await store.load() == 42
    await store.save(100)
    assert await store.load() == 100
    assert store.get() == 100
