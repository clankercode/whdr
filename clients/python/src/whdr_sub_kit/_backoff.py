"""Exponential backoff with jitter for reconnect scheduling.

Mirrors the reference (Rust ``whdr-sub-kit``) schedule exactly: the pre-jitter
base delay is ``initial * multiplier**attempt`` capped at ``max``; jitter then
multiplies by a random factor in ``[1 - jitter, 1 + jitter)``.
"""

from __future__ import annotations

import random
from dataclasses import dataclass

__all__ = ["BackoffPolicy", "Backoff"]


@dataclass(frozen=True, slots=True)
class BackoffPolicy:
    """An exponential-backoff-with-jitter schedule.

    Attributes:
        initial: Delay before the first reconnect attempt (seconds).
        max: Upper bound on the pre-jitter delay (seconds).
        multiplier: Growth factor applied per attempt.
        jitter: Jitter fraction in ``[0.0, 1.0)``. ``0.2`` means +/-20%.
    """

    initial: float = 0.5
    max: float = 30.0
    multiplier: float = 2.0
    jitter: float = 0.2

    def start(self) -> Backoff:
        """Begin a fresh backoff run at ``attempt = 0``."""
        return Backoff(self)

    def _base_delay(self, attempt: int) -> float:
        return min(self.initial * (self.multiplier**attempt), self.max)


class Backoff:
    """Running state for a :class:`BackoffPolicy`."""

    __slots__ = ("_policy", "_attempt", "_rng")

    def __init__(self, policy: BackoffPolicy, rng: random.Random | None = None) -> None:
        self._policy = policy
        self._attempt = 0
        self._rng = rng or random

    def reset(self) -> None:
        """Reset to the initial delay (call after a successful connection)."""
        self._attempt = 0

    def next_delay(self) -> float:
        """Compute the next delay (with jitter) and advance the attempt counter."""
        base = self._policy._base_delay(self._attempt)
        self._attempt += 1
        return _apply_jitter(base, self._policy.jitter, self._rng.random())


def _apply_jitter(base: float, jitter: float, rand01: float) -> float:
    """Pure jitter application, factored out for testability.

    ``rand01`` is a sample in ``[0, 1)``; the factor spans
    ``[1 - jitter, 1 + jitter)``.
    """
    if jitter <= 0.0:
        return base
    factor = 1.0 - jitter + rand01 * 2.0 * jitter
    return base * factor
