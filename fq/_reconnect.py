"""Reconnection policy and call deadlines."""

from __future__ import annotations

import random
import time
from collections.abc import Callable
from dataclasses import dataclass

from fq.errors import FQTimeoutError


@dataclass(frozen=True, slots=True)
class ReconnectPolicy:
    """Exponential backoff with full jitter.

    Attempts are unlimited by default: a server that went down should not force
    an application restart. What bounds a request is the call deadline, not an
    attempt counter; streams have no deadline and so reconnect indefinitely.
    """

    initial: float = 0.05
    max_delay: float = 2.0
    multiplier: float = 2.0
    max_attempts: int | None = None

    def delay(self, attempt: int, rand: Callable[[], float] = random.random) -> float:
        """Delay before attempt number ``attempt``, counting from one."""
        ceiling = min(self.max_delay, self.initial * self.multiplier ** (attempt - 1))

        return rand() * ceiling

    def exhausted(self, attempt: int) -> bool:
        """Whether the attempt budget is used up."""
        return self.max_attempts is not None and attempt > self.max_attempts


class Deadline:
    """The point in time after which a call is overdue."""

    __slots__ = ("_clock", "_deadline")

    def __init__(
        self,
        timeout: float | None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._clock = clock
        self._deadline = None if timeout is None else clock() + timeout

    def remaining(self) -> float | None:
        """Seconds left, or ``None`` when there is no deadline."""
        if self._deadline is None:
            return None

        return self._deadline - self._clock()

    def expired(self) -> bool:
        remaining = self.remaining()

        return remaining is not None and remaining <= 0

    def check(self) -> None:
        """Raise ``FQTimeoutError`` when the time is up."""
        if self.expired():
            raise FQTimeoutError("deadline exceeded")

    def clamp(self, delay: float) -> float:
        """Trim a delay so it never sleeps past the deadline."""
        remaining = self.remaining()
        if remaining is None:
            return delay

        return min(delay, max(remaining, 0.0))
