"""Exponential backoff with jitter."""

from __future__ import annotations

import random

from whdr_sub_kit import BackoffPolicy
from whdr_sub_kit._backoff import _apply_jitter


def test_base_delays_grow_and_cap() -> None:
    policy = BackoffPolicy(initial=0.5, max=8.0, multiplier=2.0, jitter=0.0)
    b = policy.start()
    # 0.5, 1, 2, 4, 8 (cap), 8 (cap)...
    assert b.next_delay() == 0.5
    assert b.next_delay() == 1.0
    assert b.next_delay() == 2.0
    assert b.next_delay() == 4.0
    assert b.next_delay() == 8.0
    assert b.next_delay() == 8.0


def test_reset_returns_to_initial() -> None:
    b = BackoffPolicy(jitter=0.0).start()
    first = b.next_delay()
    b.next_delay()
    b.next_delay()
    b.reset()
    assert b.next_delay() == first


def test_jitter_stays_within_bounds() -> None:
    base = 1.0
    lo = _apply_jitter(base, 0.2, 0.0)
    hi = _apply_jitter(base, 0.2, 0.9999)
    assert 0.8 <= lo <= base
    assert base <= hi < 1.2
    # Zero jitter is exact.
    assert _apply_jitter(base, 0.0, 0.5) == base


def test_next_delay_applies_jitter_via_rng() -> None:
    # A seeded RNG makes the jittered delay deterministic and within bounds.
    policy = BackoffPolicy(initial=1.0, max=100.0, multiplier=1.0, jitter=0.5)
    from whdr_sub_kit._backoff import Backoff

    b = Backoff(policy, rng=random.Random(1234))
    for _ in range(10):
        delay = b.next_delay()
        assert 0.5 <= delay < 1.5  # base 1.0, jitter +/-50%
