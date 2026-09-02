from collections.abc import Callable

import pytest

from fq._reconnect import Deadline, ReconnectPolicy
from fq.errors import FQTimeoutError


def constant(value: float) -> Callable[[], float]:
    def rand() -> float:
        return value

    return rand


def test_delay_grows_exponentially_with_full_jitter_at_the_ceiling() -> None:
    policy = ReconnectPolicy(initial=0.05, multiplier=2.0, max_delay=2.0)
    rand = constant(1.0)

    assert policy.delay(1, rand) == pytest.approx(0.05)
    assert policy.delay(2, rand) == pytest.approx(0.10)
    assert policy.delay(3, rand) == pytest.approx(0.20)


def test_delay_is_capped_by_max_delay() -> None:
    policy = ReconnectPolicy(initial=0.05, multiplier=2.0, max_delay=2.0)

    assert policy.delay(20, constant(1.0)) == pytest.approx(2.0)


def test_jitter_scales_the_whole_interval() -> None:
    policy = ReconnectPolicy(initial=0.05, multiplier=2.0, max_delay=2.0)

    assert policy.delay(3, constant(0.0)) == pytest.approx(0.0)
    assert policy.delay(3, constant(0.5)) == pytest.approx(0.10)


def test_attempts_are_unlimited_by_default() -> None:
    policy = ReconnectPolicy()

    assert policy.exhausted(1_000_000) is False


def test_attempts_can_be_bounded() -> None:
    policy = ReconnectPolicy(max_attempts=3)

    assert policy.exhausted(3) is False
    assert policy.exhausted(4) is True


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now


def test_deadline_without_a_timeout_never_expires() -> None:
    clock = FakeClock()
    deadline = Deadline(None, clock)
    clock.now = 10_000.0

    assert deadline.remaining() is None
    assert deadline.expired() is False
    deadline.check()


def test_deadline_counts_down() -> None:
    clock = FakeClock()
    deadline = Deadline(5.0, clock)
    clock.now = 2.0

    assert deadline.remaining() == pytest.approx(3.0)
    assert deadline.expired() is False


def test_expired_deadline_raises_on_check() -> None:
    clock = FakeClock()
    deadline = Deadline(5.0, clock)
    clock.now = 5.0

    assert deadline.expired() is True
    with pytest.raises(FQTimeoutError):
        deadline.check()


def test_clamp_never_sleeps_past_the_deadline() -> None:
    clock = FakeClock()
    deadline = Deadline(1.0, clock)
    clock.now = 0.6

    assert deadline.clamp(2.0) == pytest.approx(0.4)


def test_clamp_without_a_deadline_keeps_the_delay() -> None:
    assert Deadline(None, FakeClock()).clamp(2.0) == pytest.approx(2.0)
