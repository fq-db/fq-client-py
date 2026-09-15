"""fq client errors.

The protocol mandates reacting to the numeric code rather than the message text,
and allows new codes to appear inside existing ranges. The exception class is
therefore picked by range (``code // 1000``), while the exact code is always
available in the ``code`` attribute.
"""

from __future__ import annotations

from enum import IntEnum


class ErrorCode(IntEnum):
    """Error codes of fq wire protocol version 1."""

    INVALID_SYMBOL = 1000
    INVALID_COMMAND = 1001
    INVALID_ARGUMENTS = 1002
    INVALID_ARGUMENTS_COUNT = 1003
    MESSAGE_SIZE_EXCEEDS_MAXIMUM = 1004
    HANDSHAKE_REQUIRED = 1010
    UNSUPPORTED_PROTOCOL_VERSION = 1011
    PROTOCOL_VERSION_ALREADY_NEGOTIATED = 1012

    KEY_CANNOT_BE_EMPTY = 2000
    KEY_LENGTH_EXCEEDS_MAXIMUM = 2001
    BATCH_IS_NOT_NUMBER = 2002
    INVALID_BATCH_SIZE = 2003
    LIMIT_IS_NOT_NUMBER = 2004
    INVALID_LIMIT = 2005
    INVALID_RATE_LIMIT_ALGORITHM = 2006
    INVALID_SCAN_COUNT = 2007
    INVALID_SCAN_CURSOR = 2008

    NOT_AUTHENTICATED = 3000
    PERMISSION_DENIED = 3001
    AUTHENTICATION_FAILED = 3002
    TOO_MANY_AUTHENTICATION_FAILURES = 3003

    QUOTA_NOT_FOUND = 4000
    QUOTA_LIMIT_MISMATCH = 4001
    QUOTA_ALREADY_ACQUIRED_DIFFERENT_SIZE = 4002
    QUOTA_IS_NOT_EMPTY = 4003
    QUOTA_LIMIT_BELOW_USED_AMOUNT = 4004
    QUOTA_OWNERSHIP_MISMATCH = 4005
    QUOTA_POLICY_MISMATCH = 4006

    SCAN_INDEX_DISABLED = 5000
    INSPECT_NOT_AVAILABLE = 5001
    INSPECT_REPORT_TOO_LARGE = 5002
    MAX_MESSAGE_SIZE_TOO_SMALL_FOR_CHUNK = 5003
    UNSUPPORTED_COMPRESSION = 5004
    READ_ONLY_REPLICA = 5005

    INTERNAL = 9000
    INTERNAL_CONFIGURATION = 9001


class FQError(Exception):
    """Base class for every client error."""


class FQConnectionError(FQError):
    """The connection dropped, could not be established, or failed the TLS handshake."""


class FQTimeoutError(FQConnectionError):
    """The call deadline expired."""


class CorruptedResponseError(FQError):
    """The server response does not follow the protocol."""


class PoolClosedError(FQError):
    """The client has been closed."""


class ShardIndexError(FQError):
    """The sharding function returned an out-of-range index."""


class ProtocolError(FQError):
    """The server answered ``err|<code>|<message>``."""

    def __init__(self, code: int, message: str) -> None:
        super().__init__(f"{code}: {message}")
        self.code = code
        self.message = message


class RequestError(ProtocolError):
    """1xxx - request parsing and protocol violations."""


class ArgumentError(ProtocolError):
    """2xxx - argument validation."""


class AuthError(ProtocolError):
    """3xxx - authentication and authorization."""


class AuthenticationFailedError(AuthError):
    """3002 - the token was rejected."""


class QuotaError(ProtocolError):
    """4xxx - quotas."""


class InstanceStateError(ProtocolError):
    """5xxx - instance state."""


class InternalError(ProtocolError):
    """9xxx - internal server error."""


_RANGES: dict[int, type[ProtocolError]] = {
    1: RequestError,
    2: ArgumentError,
    3: AuthError,
    4: QuotaError,
    5: InstanceStateError,
    9: InternalError,
}


def protocol_error(code: int, message: str) -> ProtocolError:
    """Build the exception class matching the server error code."""
    if code == ErrorCode.AUTHENTICATION_FAILED:
        return AuthenticationFailedError(code, message)

    return _RANGES.get(code // 1000, ProtocolError)(code, message)
