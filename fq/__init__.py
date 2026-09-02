"""Python client for the fq database."""

from fq._reconnect import ReconnectPolicy
from fq._sharding import ShardingFunc, fnv1a_shard
from fq.aio.client import AsyncClient
from fq.aio.sharded import AsyncShardedClient
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
from fq.inspect_types import InspectReport
from fq.sync.client import Client
from fq.sync.sharded import ShardedClient
from fq.types import (
    PROTOCOL_VERSION,
    SCAN_CURSOR_INITIAL,
    CappingKey,
    InspectSection,
    LimitEvent,
    LimitKey,
    QuotaAcquireResult,
    QuotaClientInfo,
    QuotaEvent,
    QuotaInfo,
    RateLimitResult,
    ScanKey,
    ScanResult,
    TLSConfig,
)

__all__ = [
    "PROTOCOL_VERSION",
    "SCAN_CURSOR_INITIAL",
    "ArgumentError",
    "AsyncClient",
    "AsyncShardedClient",
    "AuthError",
    "AuthenticationFailedError",
    "CappingKey",
    "Client",
    "CorruptedResponseError",
    "ErrorCode",
    "FQConnectionError",
    "FQError",
    "FQTimeoutError",
    "InspectReport",
    "InspectSection",
    "InstanceStateError",
    "InternalError",
    "LimitEvent",
    "LimitKey",
    "PoolClosedError",
    "ProtocolError",
    "QuotaAcquireResult",
    "QuotaClientInfo",
    "QuotaError",
    "QuotaEvent",
    "QuotaInfo",
    "RateLimitResult",
    "ReconnectPolicy",
    "RequestError",
    "ScanKey",
    "ScanResult",
    "ShardIndexError",
    "ShardedClient",
    "ShardingFunc",
    "TLSConfig",
    "fnv1a_shard",
]
