import pytest

from fq._responses import (
    HelloInfo,
    ok_data,
    parse_bool,
    parse_bools,
    parse_hello,
    parse_uint,
    parse_value,
    parse_values,
    split_response,
)
from fq.errors import ArgumentError, AuthenticationFailedError, CorruptedResponseError, ErrorCode


def test_response_splits_into_tag_and_body() -> None:
    assert split_response(b"ok|42") == (b"ok", b"42")


def test_response_without_a_delimiter_is_corrupted() -> None:
    with pytest.raises(CorruptedResponseError):
        split_response(b"ok 42")


def test_unknown_tag_is_corrupted() -> None:
    with pytest.raises(CorruptedResponseError):
        ok_data(b"maybe|42")


def test_error_response_raises_the_mapped_exception() -> None:
    with pytest.raises(ArgumentError) as excinfo:
        ok_data(b"err|2000|key cannot be empty")

    assert excinfo.value.code == ErrorCode.KEY_CANNOT_BE_EMPTY
    assert excinfo.value.message == "key cannot be empty"


def test_error_message_may_contain_the_delimiter() -> None:
    with pytest.raises(ArgumentError) as excinfo:
        ok_data(b"err|2000|bad key: a|b")

    assert excinfo.value.message == "bad key: a|b"


def test_error_without_a_message_is_corrupted() -> None:
    with pytest.raises(CorruptedResponseError):
        ok_data(b"err|2000")


def test_non_numeric_error_code_is_corrupted() -> None:
    with pytest.raises(CorruptedResponseError):
        ok_data(b"err|abcd|nope")


def test_value_is_parsed() -> None:
    assert parse_value(b"ok|42") == 42


def test_uint64_maximum_survives() -> None:
    assert parse_value(b"ok|18446744073709551615") == 18446744073709551615


def test_negative_value_is_corrupted() -> None:
    with pytest.raises(CorruptedResponseError):
        parse_value(b"ok|-1")


def test_empty_value_is_corrupted() -> None:
    with pytest.raises(CorruptedResponseError):
        parse_value(b"ok|")


def test_non_ascii_digits_are_corrupted() -> None:
    with pytest.raises(CorruptedResponseError):
        parse_uint("٤٢".encode())


def test_bool_response_maps_one_to_true() -> None:
    assert parse_bool(b"ok|1") is True
    assert parse_bool(b"ok|0") is False


def test_multi_value_response_is_split_on_semicolons() -> None:
    assert parse_values(b"ok|1;0;1") == (1, 0, 1)


def test_single_value_multi_response_works() -> None:
    assert parse_values(b"ok|1") == (1,)


def test_multi_bool_response_is_mapped() -> None:
    assert parse_bools(b"ok|1;0;1") == (True, False, True)


def test_hello_response_is_parsed() -> None:
    assert parse_hello(b"ok|1;65536;1;admin") == HelloInfo(
        version=1,
        max_message_size=65536,
        auth_required=True,
        role="admin",
    )


def test_hello_with_zero_max_message_size_is_corrupted() -> None:
    with pytest.raises(CorruptedResponseError):
        parse_hello(b"ok|1;0;0;admin")


def test_hello_with_a_missing_field_is_corrupted() -> None:
    with pytest.raises(CorruptedResponseError):
        parse_hello(b"ok|1;65536;1")


def test_rejected_token_raises_authentication_failed() -> None:
    with pytest.raises(AuthenticationFailedError):
        parse_hello(b"err|3002|authentication failed")
