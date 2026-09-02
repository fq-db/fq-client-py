"""Response decoders for the fq wire protocol.

Every function takes a whole frame payload (``ok|...`` or ``err|...``) and
returns the parsed value. An ``err|`` response becomes the matching exception,
anything that deviates from the format becomes ``CorruptedResponseError``.
"""

from __future__ import annotations

from dataclasses import dataclass

from fq.errors import CorruptedResponseError, ProtocolError, protocol_error
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

RESP_DELIMITER = b"|"
FIELD_DELIMITER = b";"

TAG_OK = b"ok"
TAG_ERR = b"err"
TAG_NXT = b"nxt"


def split_response(payload: bytes) -> tuple[bytes, bytes]:
    """Split a response into its tag and body."""
    tag, delimiter, data = payload.partition(RESP_DELIMITER)
    if not delimiter:
        raise CorruptedResponseError(f"response without a delimiter: {payload!r}")

    return tag, data


def parse_error(data: bytes) -> ProtocolError:
    """Turn the body of an ``err|`` response into an exception."""
    code_field, delimiter, message = data.partition(RESP_DELIMITER)
    if not delimiter:
        raise CorruptedResponseError(f"error response without a message: {data!r}")

    code = parse_uint(code_field)
    if code > 9999:
        raise CorruptedResponseError(f"error code out of range: {code}")

    return protocol_error(code, message.decode("utf-8", "replace"))


def ok_data(payload: bytes) -> bytes:
    """Return the body of a successful response, or raise."""
    tag, data = split_response(payload)
    if tag == TAG_OK:
        return data
    if tag == TAG_ERR:
        raise parse_error(data)

    raise CorruptedResponseError(f"unknown response tag: {tag!r}")


def parse_uint(field: bytes) -> int:
    """Parse an unsigned integer written as decimal ASCII digits."""
    if not field or not field.isdigit() or not field.isascii():
        raise CorruptedResponseError(f"not an unsigned integer: {field!r}")

    return int(field)


def parse_bool_field(field: bytes) -> bool:
    """Parse a boolean field, which the protocol encodes as 0 or 1."""
    if field == b"1":
        return True
    if field == b"0":
        return False

    raise CorruptedResponseError(f"not a boolean field: {field!r}")


def fields(data: bytes, expected: int) -> list[bytes]:
    """Split a response body into fields and check how many there are."""
    parts = data.split(FIELD_DELIMITER)
    if len(parts) != expected:
        raise CorruptedResponseError(f"expected {expected} fields, got {len(parts)}: {data!r}")

    return parts


def parse_value(payload: bytes) -> int:
    """A response shaped ``ok|<number>``."""
    return parse_uint(ok_data(payload))


def parse_bool(payload: bytes) -> bool:
    """A response shaped ``ok|0`` or ``ok|1``."""
    return parse_bool_field(ok_data(payload))


def parse_values(payload: bytes) -> tuple[int, ...]:
    """A response shaped ``ok|<number>;<number>;...``."""
    return tuple(parse_uint(field) for field in ok_data(payload).split(FIELD_DELIMITER))


def parse_bools(payload: bytes) -> tuple[bool, ...]:
    """The MDEL response: several boolean values."""
    return tuple(value == 1 for value in parse_values(payload))


@dataclass(frozen=True, slots=True)
class HelloInfo:
    """The negotiated connection parameters."""

    version: int
    max_message_size: int
    auth_required: bool
    role: str


def parse_hello(payload: bytes) -> HelloInfo:
    """A response shaped ``ok|<version>;<max_message_size>;<auth_required>;<role>``."""
    version, max_message_size, auth_required, role = fields(ok_data(payload), 4)

    size = parse_uint(max_message_size)
    if size == 0:
        raise CorruptedResponseError("max_message_size must be positive")

    return HelloInfo(
        version=parse_uint(version),
        max_message_size=size,
        auth_required=parse_bool_field(auth_required),
        role=role.decode("utf-8", "replace"),
    )


def _text(field: bytes) -> str:
    return field.decode("utf-8", "replace")


def parse_rate_limit(payload: bytes) -> RateLimitResult:
    """A response shaped ``ok|<allowed>;<current>;<remaining>;<reset_after>``."""
    allowed, current, remaining, reset_after = fields(ok_data(payload), 4)

    return RateLimitResult(
        allowed=parse_bool_field(allowed),
        current=parse_uint(current),
        remaining=parse_uint(remaining),
        reset_after=parse_uint(reset_after),
    )


def parse_quota_acquire(payload: bytes) -> QuotaAcquireResult:
    """A response shaped ``ok|<acquired>;<allocated>;<used>;<remaining>;<expires_after>``."""
    acquired, allocated, used, remaining, expires_after = fields(ok_data(payload), 5)

    return QuotaAcquireResult(
        acquired=parse_bool_field(acquired),
        allocated=parse_uint(allocated),
        used=parse_uint(used),
        remaining=parse_uint(remaining),
        expires_after=parse_uint(expires_after),
    )


def parse_quota_info(payload: bytes) -> QuotaInfo:
    """A response shaped ``ok|<limit>;<used>;<remaining>`` plus client triples."""
    parts = ok_data(payload).split(FIELD_DELIMITER)
    if len(parts) < 3 or (len(parts) - 3) % 3 != 0:
        raise CorruptedResponseError(f"malformed quota info response: {payload!r}")

    clients = tuple(
        QuotaClientInfo(
            client_id=_text(parts[index]),
            amount=parse_uint(parts[index + 1]),
            expires_at=parse_uint(parts[index + 2]),
        )
        for index in range(3, len(parts), 3)
    )

    return QuotaInfo(
        limit=parse_uint(parts[0]),
        used=parse_uint(parts[1]),
        remaining=parse_uint(parts[2]),
        clients=clients,
    )


def parse_scan(payload: bytes) -> ScanResult:
    """A response shaped ``ok|<next_cursor>[;<key>;<window>...]``."""
    parts = ok_data(payload).split(FIELD_DELIMITER)
    if (len(parts) - 1) % 2 != 0:
        raise CorruptedResponseError(f"malformed scan response: {payload!r}")

    keys = tuple(
        ScanKey(key=_text(parts[index]), window=parse_uint(parts[index + 1]))
        for index in range(1, len(parts), 2)
    )

    return ScanResult(cursor=_text(parts[0]), keys=keys)


def parse_limit_event(payload: bytes) -> LimitEvent:
    """A stream frame shaped ``ok|<key>;<window>;<current>;<reset_after>``."""
    key, window, current, reset_after = fields(ok_data(payload), 4)

    return LimitEvent(
        key=_text(key),
        window=parse_uint(window),
        current=parse_uint(current),
        reset_after=parse_uint(reset_after),
    )


def parse_quota_event(payload: bytes) -> QuotaEvent:
    """A stream frame with event, name, client id and four numbers."""
    event, name, client_id, amount, used, remaining, expires_at = fields(ok_data(payload), 7)

    return QuotaEvent(
        event=_text(event),
        name=_text(name),
        client_id=_text(client_id),
        amount=parse_uint(amount),
        used=parse_uint(used),
        remaining=parse_uint(remaining),
        expires_at=parse_uint(expires_at),
    )
