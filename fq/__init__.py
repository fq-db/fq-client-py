"""Python client for the fq database."""

from fq.errors import (
    ArgumentError,
    AuthenticationFailedError,
    AuthError,
    CorruptedResponseError,
    ErrorCode,
    FQConnectionError,
    FQError,
    FQTimeoutError,
    InstanceStateError,
    InternalError,
    PoolClosedError,
    ProtocolError,
    QuotaError,
    RequestError,
    ShardIndexError,
)

__all__ = [
    "ArgumentError",
    "AuthError",
    "AuthenticationFailedError",
    "CorruptedResponseError",
    "ErrorCode",
    "FQConnectionError",
    "FQError",
    "FQTimeoutError",
    "InstanceStateError",
    "InternalError",
    "PoolClosedError",
    "ProtocolError",
    "QuotaError",
    "RequestError",
    "ShardIndexError",
]
