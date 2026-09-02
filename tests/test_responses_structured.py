import pytest

from fq._responses import (
    parse_limit_event,
    parse_quota_acquire,
    parse_quota_event,
    parse_quota_info,
    parse_rate_limit,
    parse_scan,
)
from fq.errors import CorruptedResponseError, InstanceStateError
from fq.types import (
    LimitEvent,
    QuotaAcquireResult,
    QuotaClientInfo,
    QuotaEvent,
    QuotaInfo,
    RateLimitResult,
    ScanKey,
    ScanResult,
)


def test_rate_limit_response_is_parsed() -> None:
    assert parse_rate_limit(b"ok|1;2;1;43") == RateLimitResult(
        allowed=True,
        current=2,
        remaining=1,
        reset_after=43,
    )


def test_rejected_rate_limit_response_is_parsed() -> None:
    assert parse_rate_limit(b"ok|0;3;0;41").allowed is False


def test_rate_limit_with_wrong_field_count_is_corrupted() -> None:
    with pytest.raises(CorruptedResponseError):
        parse_rate_limit(b"ok|1;2;1")


def test_quota_acquire_response_is_parsed() -> None:
    assert parse_quota_acquire(b"ok|1;4;4;6;0") == QuotaAcquireResult(
        acquired=True,
        allocated=4,
        used=4,
        remaining=6,
        expires_after=0,
    )


def test_quota_info_without_clients_is_parsed() -> None:
    assert parse_quota_info(b"ok|10;0;10") == QuotaInfo(10, 0, 10, ())


def test_quota_info_with_clients_is_parsed() -> None:
    payload = b"ok|10;7;3;client-a;4;1700000000;client-b;3;0"

    assert parse_quota_info(payload) == QuotaInfo(
        limit=10,
        used=7,
        remaining=3,
        clients=(
            QuotaClientInfo("client-a", 4, 1700000000),
            QuotaClientInfo("client-b", 3, 0),
        ),
    )


def test_quota_info_with_a_truncated_client_triple_is_corrupted() -> None:
    with pytest.raises(CorruptedResponseError):
        parse_quota_info(b"ok|10;7;3;client-a;4")


def test_scan_response_without_keys_is_parsed() -> None:
    assert parse_scan(b"ok|0") == ScanResult("0", ())


def test_scan_response_with_keys_is_parsed() -> None:
    assert parse_scan(b"ok|17;user_1;60;user_2;30") == ScanResult(
        cursor="17",
        keys=(ScanKey("user_1", 60), ScanKey("user_2", 30)),
    )


def test_scan_response_with_a_dangling_key_is_corrupted() -> None:
    with pytest.raises(CorruptedResponseError):
        parse_scan(b"ok|17;user_1")


def test_disabled_scan_index_surfaces_as_a_protocol_error() -> None:
    with pytest.raises(InstanceStateError):
        parse_scan(b"err|5000|scan index is disabled")


def test_limit_event_is_parsed() -> None:
    assert parse_limit_event(b"ok|user_42;60;100;12") == LimitEvent(
        key="user_42",
        window=60,
        current=100,
        reset_after=12,
    )


def test_quota_event_is_parsed() -> None:
    assert parse_quota_event(b"ok|acq;plan_a;client-1;4;7;3;1700000000") == QuotaEvent(
        event="acq",
        name="plan_a",
        client_id="client-1",
        amount=4,
        used=7,
        remaining=3,
        expires_at=1700000000,
    )


def test_quota_delete_event_has_an_empty_client_id() -> None:
    assert parse_quota_event(b"ok|del;plan_a;;0;0;0;0").client_id == ""
