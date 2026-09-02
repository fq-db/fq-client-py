import pytest

from fq._framing import FRAME_HEADER_SIZE, check_request_size, decode_frame_size, encode_frame
from fq.errors import CorruptedResponseError, ErrorCode, RequestError


def test_frame_is_length_prefixed_big_endian() -> None:
    assert encode_frame(b"HELLO 1") == b"\x00\x00\x00\x07HELLO 1"


def test_empty_payload_still_gets_a_header() -> None:
    assert encode_frame(b"") == b"\x00\x00\x00\x00"


def test_header_size_is_four_bytes() -> None:
    assert FRAME_HEADER_SIZE == 4


def test_frame_size_is_read_from_the_header() -> None:
    assert decode_frame_size(b"\x00\x00\x10\x00", max_message_size=4096) == 4096


def test_frame_larger_than_the_limit_is_rejected() -> None:
    with pytest.raises(CorruptedResponseError):
        decode_frame_size(b"\x00\x00\x10\x01", max_message_size=4096)


def test_short_header_is_rejected() -> None:
    with pytest.raises(CorruptedResponseError):
        decode_frame_size(b"\x00\x00", max_message_size=4096)


def test_oversized_request_is_refused_locally() -> None:
    with pytest.raises(RequestError) as excinfo:
        check_request_size(b"x" * 4097, max_message_size=4096)

    assert excinfo.value.code == ErrorCode.MESSAGE_SIZE_EXCEEDS_MAXIMUM


def test_request_at_the_limit_is_allowed() -> None:
    check_request_size(b"x" * 4096, max_message_size=4096)
