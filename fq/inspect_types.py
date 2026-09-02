"""INSPECT report types and their JSON decoding.

The report arrives as JSON. Decoding is manual and lenient: unknown fields are
ignored and absent sections become ``None``, so a newer server does not break an
older client.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

from fq._responses import ok_data
from fq.errors import CorruptedResponseError


@dataclass(frozen=True, slots=True)
class InstanceInfo:
    version: str = ""
    commit: str = ""
    build_date: str = ""
    go_version: str = ""
    platform: str = ""
    uptime_sec: float = 0.0
    pid: int = 0
    role: str = ""
    replica_id: str | None = None
    listen_addr: str = ""
    connections: int = 0


@dataclass(frozen=True, slots=True)
class PersistenceInfo:
    mode: str = ""
    sync_commit: str | None = None


@dataclass(frozen=True, slots=True)
class WALInfo:
    enabled: bool = False
    sync_commit: str | None = None
    data_directory: str | None = None
    queue_depth: int | None = None
    queue_capacity: int | None = None
    lsn_assigned: int = 0
    lsn_flushed: int | None = None
    segment_path: str | None = None
    segment_size_bytes: int | None = None
    last_flush_at: int | None = None
    last_flush_duration_ms: float | None = None
    flush_avg_duration_ms: float | None = None
    flush_total: float | None = None


@dataclass(frozen=True, slots=True)
class DumpInfo:
    enabled: bool = False
    directory: str | None = None
    interval_sec: float | None = None
    last_dump_at: int | None = None
    last_dump_duration_ms: float | None = None
    last_dump_error: str | None = None
    last_dump_tx: int | None = None
    current_dump_path: str | None = None
    current_dump_size_bytes: int | None = None
    next_dump_at: int | None = None


@dataclass(frozen=True, slots=True)
class ReplicaInfo:
    replica_id: str = ""
    last_segment_name: str = ""
    last_applied_lsn: int = 0
    lag_lsn: int = 0
    lag_sec: float = 0.0
    stale: bool = False
    updated_at: int = 0


@dataclass(frozen=True, slots=True)
class SlaveInfo:
    master_address: str = ""
    connected: bool = False
    last_segment_name: str = ""
    last_applied_lsn: int = 0
    consecutive_errors: int = 0
    reconnect_total: int = 0
    last_reconnect_at: int | None = None
    updated_at: int = 0


@dataclass(frozen=True, slots=True)
class ReplInfo:
    role: str = ""
    known_replicas: int = 0
    replicas: tuple[ReplicaInfo, ...] = ()
    slave: SlaveInfo | None = None
    truncated: bool = False


@dataclass(frozen=True, slots=True)
class PartitionStat:
    index: int = 0
    counters: int = 0
    sliding_windows: int = 0
    token_buckets: int = 0
    quotas: int = 0
    quota_allocations: int = 0


@dataclass(frozen=True, slots=True)
class EngineInfo:
    partitions: int = 0
    counters: int = 0
    sliding_windows: int = 0
    token_buckets: int = 0
    quotas: int = 0
    quota_allocations: int = 0
    key_index_enabled: bool = False
    per_partition: tuple[PartitionStat, ...] = ()


@dataclass(frozen=True, slots=True)
class SubscriberInfo:
    prefix: str | None = None
    queue_len: int = 0
    queue_cap: int = 0
    dropped: int = 0


@dataclass(frozen=True, slots=True)
class StreamsInfo:
    limit_subscribers: tuple[SubscriberInfo, ...] = ()
    quota_subscribers: tuple[SubscriberInfo, ...] = ()
    limit_events_dropped_total: int = 0
    quota_events_dropped_total: int = 0


@dataclass(frozen=True, slots=True)
class InspectWarning:
    code: str = ""
    severity: str = ""
    message: str = ""
    details: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class InspectReport:
    section: str = ""
    ts: int = 0
    instance: InstanceInfo | None = None
    persistence: PersistenceInfo | None = None
    wal: WALInfo | None = None
    dump: DumpInfo | None = None
    repl: ReplInfo | None = None
    engine: EngineInfo | None = None
    streams: StreamsInfo | None = None
    warnings: tuple[InspectWarning, ...] = ()


def _section(raw: dict[str, Any], key: str) -> dict[str, Any] | None:
    value = raw.get(key)

    return value if isinstance(value, dict) else None


def _items(raw: dict[str, Any], key: str) -> list[dict[str, Any]]:
    value = raw.get(key)
    if not isinstance(value, list):
        return []

    return [item for item in value if isinstance(item, dict)]


def _instance(raw: dict[str, Any]) -> InstanceInfo:
    return InstanceInfo(
        version=raw.get("version", ""),
        commit=raw.get("commit", ""),
        build_date=raw.get("build_date", ""),
        go_version=raw.get("go_version", ""),
        platform=raw.get("platform", ""),
        uptime_sec=raw.get("uptime_sec", 0.0),
        pid=raw.get("pid", 0),
        role=raw.get("role", ""),
        replica_id=raw.get("replica_id"),
        listen_addr=raw.get("listen_addr", ""),
        connections=raw.get("connections", 0),
    )


def _wal(raw: dict[str, Any]) -> WALInfo:
    return WALInfo(
        enabled=raw.get("enabled", False),
        sync_commit=raw.get("sync_commit"),
        data_directory=raw.get("data_directory"),
        queue_depth=raw.get("queue_depth"),
        queue_capacity=raw.get("queue_capacity"),
        lsn_assigned=raw.get("lsn_assigned", 0),
        lsn_flushed=raw.get("lsn_flushed"),
        segment_path=raw.get("segment_path"),
        segment_size_bytes=raw.get("segment_size_bytes"),
        last_flush_at=raw.get("last_flush_at"),
        last_flush_duration_ms=raw.get("last_flush_duration_ms"),
        flush_avg_duration_ms=raw.get("flush_avg_duration_ms"),
        flush_total=raw.get("flush_total"),
    )


def _dump(raw: dict[str, Any]) -> DumpInfo:
    return DumpInfo(
        enabled=raw.get("enabled", False),
        directory=raw.get("directory"),
        interval_sec=raw.get("interval_sec"),
        last_dump_at=raw.get("last_dump_at"),
        last_dump_duration_ms=raw.get("last_dump_duration_ms"),
        last_dump_error=raw.get("last_dump_error"),
        last_dump_tx=raw.get("last_dump_tx"),
        current_dump_path=raw.get("current_dump_path"),
        current_dump_size_bytes=raw.get("current_dump_size_bytes"),
        next_dump_at=raw.get("next_dump_at"),
    )


def _slave(raw: dict[str, Any]) -> SlaveInfo:
    return SlaveInfo(
        master_address=raw.get("master_address", ""),
        connected=raw.get("connected", False),
        last_segment_name=raw.get("last_segment_name", ""),
        last_applied_lsn=raw.get("last_applied_lsn", 0),
        consecutive_errors=raw.get("consecutive_errors", 0),
        reconnect_total=raw.get("reconnect_total", 0),
        last_reconnect_at=raw.get("last_reconnect_at"),
        updated_at=raw.get("updated_at", 0),
    )


def _repl(raw: dict[str, Any]) -> ReplInfo:
    slave = _section(raw, "slave")

    return ReplInfo(
        role=raw.get("role", ""),
        known_replicas=raw.get("known_replicas", 0),
        replicas=tuple(
            ReplicaInfo(
                replica_id=item.get("replica_id", ""),
                last_segment_name=item.get("last_segment_name", ""),
                last_applied_lsn=item.get("last_applied_lsn", 0),
                lag_lsn=item.get("lag_lsn", 0),
                lag_sec=item.get("lag_sec", 0.0),
                stale=item.get("stale", False),
                updated_at=item.get("updated_at", 0),
            )
            for item in _items(raw, "replicas")
        ),
        slave=None if slave is None else _slave(slave),
        truncated=raw.get("truncated", False),
    )


def _engine(raw: dict[str, Any]) -> EngineInfo:
    return EngineInfo(
        partitions=raw.get("partitions", 0),
        counters=raw.get("counters", 0),
        sliding_windows=raw.get("sliding_windows", 0),
        token_buckets=raw.get("token_buckets", 0),
        quotas=raw.get("quotas", 0),
        quota_allocations=raw.get("quota_allocations", 0),
        key_index_enabled=raw.get("key_index_enabled", False),
        per_partition=tuple(
            PartitionStat(
                index=item.get("index", 0),
                counters=item.get("counters", 0),
                sliding_windows=item.get("sliding_windows", 0),
                token_buckets=item.get("token_buckets", 0),
                quotas=item.get("quotas", 0),
                quota_allocations=item.get("quota_allocations", 0),
            )
            for item in _items(raw, "per_partition")
        ),
    )


def _subscribers(raw: dict[str, Any], key: str) -> tuple[SubscriberInfo, ...]:
    return tuple(
        SubscriberInfo(
            prefix=item.get("prefix"),
            queue_len=item.get("queue_len", 0),
            queue_cap=item.get("queue_cap", 0),
            dropped=item.get("dropped", 0),
        )
        for item in _items(raw, key)
    )


def _streams(raw: dict[str, Any]) -> StreamsInfo:
    return StreamsInfo(
        limit_subscribers=_subscribers(raw, "limit_subscribers"),
        quota_subscribers=_subscribers(raw, "quota_subscribers"),
        limit_events_dropped_total=raw.get("limit_events_dropped_total", 0),
        quota_events_dropped_total=raw.get("quota_events_dropped_total", 0),
    )


def parse_inspect(payload: bytes) -> InspectReport:
    """Decode the response of the INSPECT command."""
    data = ok_data(payload)

    try:
        raw = json.loads(data)
    except ValueError as error:
        raise CorruptedResponseError(f"inspect report is not valid JSON: {error}") from error

    if not isinstance(raw, dict):
        raise CorruptedResponseError("inspect report is not a JSON object")

    persistence = _section(raw, "persistence")
    instance = _section(raw, "instance")
    wal = _section(raw, "wal")
    dump = _section(raw, "dump")
    repl = _section(raw, "repl")
    engine = _section(raw, "engine")
    streams = _section(raw, "streams")

    return InspectReport(
        section=raw.get("section", ""),
        ts=raw.get("ts", 0),
        instance=None if instance is None else _instance(instance),
        persistence=None
        if persistence is None
        else PersistenceInfo(
            mode=persistence.get("mode", ""),
            sync_commit=persistence.get("sync_commit"),
        ),
        wal=None if wal is None else _wal(wal),
        dump=None if dump is None else _dump(dump),
        repl=None if repl is None else _repl(repl),
        engine=None if engine is None else _engine(engine),
        streams=None if streams is None else _streams(streams),
        warnings=tuple(
            InspectWarning(
                code=item.get("code", ""),
                severity=item.get("severity", ""),
                message=item.get("message", ""),
                details=item.get("details") or {},
            )
            for item in _items(raw, "warnings")
        ),
    )
