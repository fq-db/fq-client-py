import pytest

from fq.errors import (
    ArgumentError,
    AuthenticationFailedError,
    AuthError,
    ErrorCode,
    InstanceStateError,
    InternalError,
    ProtocolError,
    QuotaError,
    RequestError,
    protocol_error,
)


@pytest.mark.parametrize(
    ("code", "expected"),
    [
        (1000, RequestError),
        (1012, RequestError),
        (2008, ArgumentError),
        (3000, AuthError),
        (3001, AuthError),
        (4006, QuotaError),
        (5000, InstanceStateError),
        (9001, InternalError),
    ],
)
def test_class_is_chosen_by_code_range(code: int, expected: type[ProtocolError]) -> None:
    assert type(protocol_error(code, "boom")) is expected


def test_authentication_failure_has_its_own_class() -> None:
    error = protocol_error(3002, "authentication failed")

    assert isinstance(error, AuthenticationFailedError)
    assert isinstance(error, AuthError)


def test_unknown_code_inside_known_range_keeps_the_range_class() -> None:
    assert type(protocol_error(4999, "future quota error")) is QuotaError


def test_unknown_range_falls_back_to_the_base_class() -> None:
    assert type(protocol_error(7000, "from the future")) is ProtocolError


def test_error_code_compares_as_a_plain_int() -> None:
    error = protocol_error(3001, "permission denied")

    assert error.code == ErrorCode.PERMISSION_DENIED
    assert error.code == 3001


def test_message_carries_code_and_text() -> None:
    error = protocol_error(2000, "key cannot be empty")

    assert str(error) == "2000: key cannot be empty"
    assert error.message == "key cannot be empty"


def test_every_documented_code_is_present() -> None:
    documented = {
        1000,
        1001,
        1002,
        1003,
        1004,
        1010,
        1011,
        1012,
        2000,
        2001,
        2002,
        2003,
        2004,
        2005,
        2006,
        2007,
        2008,
        3000,
        3001,
        3002,
        3003,
        4000,
        4001,
        4002,
        4003,
        4004,
        4005,
        4006,
        5000,
        5001,
        5002,
        5003,
        5004,
        5005,
        9000,
        9001,
    }

    assert {int(code) for code in ErrorCode} == documented


@pytest.mark.parametrize(
    ("code", "member"),
    [
        (5004, ErrorCode.UNSUPPORTED_COMPRESSION),
        (5005, ErrorCode.READ_ONLY_REPLICA),
    ],
)
def test_replication_codes_map_to_instance_state_errors(code: int, member: ErrorCode) -> None:
    error = protocol_error(code, "boom")

    assert type(error) is InstanceStateError
    assert error.code == member
