import json

import pytest

from fq.errors import CorruptedResponseError, InstanceStateError
from fq.inspect_types import parse_inspect


def payload(report: dict[str, object]) -> bytes:
    return b"ok|" + json.dumps(report).encode()


def test_summary_report_is_parsed() -> None:
    report = parse_inspect(
        payload(
            {
                "section": "",
                "ts": 1700000000,
                "instance": {
                    "version": "1.2.3",
                    "commit": "abc123",
                    "build_date": "2026-01-01",
                    "go_version": "go1.22",
                    "platform": "linux/amd64",
                    "uptime_sec": 42.5,
                    "pid": 7,
                    "role": "master",
                    "replica_id": None,
                    "listen_addr": ":1945",
                    "connections": 3,
                },
            }
        )
    )

    assert report.ts == 1700000000
    assert report.instance is not None
    assert report.instance.version == "1.2.3"
    assert report.instance.uptime_sec == pytest.approx(42.5)
    assert report.instance.replica_id is None
    assert report.wal is None


def test_wal_section_is_parsed() -> None:
    report = parse_inspect(
        payload(
            {
                "section": "WAL",
                "ts": 1,
                "wal": {
                    "enabled": True,
                    "sync_commit": "off",
                    "data_directory": "./fq_data/wal/",
                    "queue_depth": 0,
                    "queue_capacity": 16384,
                    "lsn_assigned": 100,
                    "lsn_flushed": 99,
                    "segment_path": "seg-1",
                    "segment_size_bytes": 1024,
                    "last_flush_at": 1700000000,
                    "last_flush_duration_ms": 1.5,
                    "flush_avg_duration_ms": 1.2,
                    "flush_total": 10.0,
                },
            }
        )
    )

    assert report.wal is not None
    assert report.wal.enabled is True
    assert report.wal.lsn_assigned == 100
    assert report.wal.lsn_flushed == 99


def test_engine_section_with_partitions_is_parsed() -> None:
    report = parse_inspect(
        payload(
            {
                "section": "ENGINE",
                "ts": 1,
                "engine": {
                    "partitions": 2,
                    "counters": 10,
                    "sliding_windows": 1,
                    "token_buckets": 2,
                    "quotas": 3,
                    "quota_allocations": 4,
                    "key_index_enabled": True,
                    "per_partition": [
                        {
                            "index": 0,
                            "counters": 5,
                            "sliding_windows": 0,
                            "token_buckets": 1,
                            "quotas": 1,
                            "quota_allocations": 2,
                        }
                    ],
                },
            }
        )
    )

    assert report.engine is not None
    assert report.engine.key_index_enabled is True
    assert len(report.engine.per_partition) == 1
    assert report.engine.per_partition[0].counters == 5


def test_replication_section_is_parsed() -> None:
    report = parse_inspect(
        payload(
            {
                "section": "REPL",
                "ts": 1,
                "repl": {
                    "role": "master",
                    "known_replicas": 1,
                    "replicas": [
                        {
                            "replica_id": "r1",
                            "last_segment_name": "seg-1",
                            "last_applied_lsn": 90,
                            "lag_lsn": 10,
                            "lag_sec": 0.5,
                            "stale": False,
                            "updated_at": 1700000000,
                        }
                    ],
                    "truncated": False,
                },
            }
        )
    )

    assert report.repl is not None
    assert report.repl.replicas[0].replica_id == "r1"
    assert report.repl.slave is None


def test_streams_section_is_parsed() -> None:
    report = parse_inspect(
        payload(
            {
                "section": "STREAMS",
                "ts": 1,
                "streams": {
                    "limit_subscribers": [
                        {"prefix": "user_", "queue_len": 1, "queue_cap": 16, "dropped": 0}
                    ],
                    "quota_subscribers": [],
                    "limit_events_dropped_total": 2,
                    "quota_events_dropped_total": 0,
                },
            }
        )
    )

    assert report.streams is not None
    assert report.streams.limit_subscribers[0].prefix == "user_"
    assert report.streams.quota_subscribers == ()
    assert report.streams.limit_events_dropped_total == 2


def test_warnings_are_parsed() -> None:
    report = parse_inspect(
        payload(
            {
                "section": "ALL",
                "ts": 1,
                "warnings": [
                    {
                        "code": "wal_queue_full",
                        "severity": "warn",
                        "message": "queue is full",
                        "details": {"depth": 16384},
                    }
                ],
            }
        )
    )

    assert report.warnings[0].code == "wal_queue_full"
    assert report.warnings[0].details == {"depth": 16384}


def test_unknown_fields_are_ignored() -> None:
    report = parse_inspect(payload({"section": "", "ts": 1, "brand_new_section": {"a": 1}}))

    assert report.ts == 1


def test_broken_json_is_corrupted() -> None:
    with pytest.raises(CorruptedResponseError):
        parse_inspect(b"ok|{not json")


def test_unavailable_inspect_surfaces_as_a_protocol_error() -> None:
    with pytest.raises(InstanceStateError):
        parse_inspect(b"err|5001|inspect is not available")
