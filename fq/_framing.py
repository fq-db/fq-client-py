"""Frame encoding for the fq wire protocol.

Every message is a four-byte big-endian length followed by exactly that many
bytes of payload.
"""

from __future__ import annotations

from fq.errors import CorruptedResponseError, ErrorCode, RequestError

FRAME_HEADER_SIZE = 4


def encode_frame(payload: bytes) -> bytes:
    """Wrap a payload in a frame."""
    return len(payload).to_bytes(FRAME_HEADER_SIZE, "big") + payload


def decode_frame_size(header: bytes, max_message_size: int) -> int:
    """Read the frame length out of a header."""
    if len(header) != FRAME_HEADER_SIZE:
        raise CorruptedResponseError(
            f"frame header must be {FRAME_HEADER_SIZE} bytes, got {len(header)}"
        )

    size = int.from_bytes(header, "big")
    if size > max_message_size:
        raise CorruptedResponseError(f"frame of {size} bytes exceeds limit {max_message_size}")

    return size


def check_request_size(payload: bytes, max_message_size: int) -> None:
    """Refuse an oversized request locally instead of spending a round trip."""
    if len(payload) > max_message_size:
        raise RequestError(
            ErrorCode.MESSAGE_SIZE_EXCEEDS_MAXIMUM,
            f"request of {len(payload)} bytes exceeds max message size {max_message_size}",
        )
