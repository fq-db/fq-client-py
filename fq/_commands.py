"""Command encoders for the fq wire protocol.

Each function returns a ``Command``: the frame payload plus a flag saying
whether the command is safe to repeat after the connection dropped with the
request already on the wire.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from fq.types import PROTOCOL_VERSION, SCAN_CURSOR_INITIAL, CappingKey, InspectSection, LimitKey


@dataclass(frozen=True, slots=True)
class Command:
    """A command payload and whether repeating it is safe."""

    payload: bytes
    read_only: bool


def _join(*parts: object) -> bytes:
    return " ".join(str(part) for part in parts).encode("utf-8")


def _read_only(*parts: object) -> Command:
    return Command(_join(*parts), read_only=True)


def _mutating(*parts: object) -> Command:
    return Command(_join(*parts), read_only=False)


def hello(token: str | None) -> Command:
    if token:
        return _read_only("HELLO", PROTOCOL_VERSION, "AUTH", token)

    return _read_only("HELLO", PROTOCOL_VERSION)


def incr(key: CappingKey) -> Command:
    return _mutating("INCR", key.key, key.capping)


def get(key: CappingKey) -> Command:
    return _read_only("GET", key.key, key.capping)


def watch(key: CappingKey) -> Command:
    return _read_only("WATCH", key.key, key.capping)


def delete(key: CappingKey) -> Command:
    return _mutating("DEL", key.key, key.capping)


def mdelete(keys: Sequence[CappingKey]) -> Command:
    parts: list[object] = ["MDEL"]
    for key in keys:
        parts.extend((key.key, key.capping))

    return _mutating(*parts)


def rlimit_fixed_window(key: LimitKey, limit: int) -> Command:
    return _mutating("RLIMIT", "FW", key.key, limit, key.window)


def rlimit_sliding_window(key: LimitKey, limit: int) -> Command:
    return _mutating("RLIMIT", "SW", key.key, limit, key.window)


def rlimit_token_bucket(key: LimitKey, capacity: int, refill_amount: int) -> Command:
    return _mutating("RLIMIT", "TB", key.key, capacity, refill_amount, key.window)


def quota_set(name: str, limit: int) -> Command:
    return _mutating("QUOTA", "SET", name, limit)


def quota_set_n(name: str, limit: int, clients: int) -> Command:
    return _mutating("QUOTA", "SETN", name, limit, clients)


def quota_acquire(name: str, amount: int, client_id: str, ttl: int | None = None) -> Command:
    parts: list[object] = ["QUOTA", "ACQ", name, amount, client_id]
    if ttl is not None:
        parts.append(ttl)

    return _mutating(*parts)


def quota_acquire_n(name: str, client_id: str, ttl: int | None = None) -> Command:
    parts: list[object] = ["QUOTA", "ACQN", name, client_id]
    if ttl is not None:
        parts.append(ttl)

    return _mutating(*parts)


def quota_acquire_lease(
    name: str,
    limit: int,
    amount: int,
    client_id: str,
    ttl: int | None = None,
) -> Command:
    parts: list[object] = ["QUOTA", "ACQL", name, limit, amount, client_id]
    if ttl is not None:
        parts.append(ttl)

    return _mutating(*parts)


def quota_release(name: str, client_id: str) -> Command:
    return _mutating("QUOTA", "REL", name, client_id)


def quota_delete(name: str) -> Command:
    return _mutating("QUOTA", "DEL", name)


def quota_info(name: str) -> Command:
    return _read_only("QUOTA", "INF", name)


def scan(count: int, cursor: str) -> Command:
    return _read_only("SCAN", cursor or SCAN_CURSOR_INITIAL, count)


def pscan(prefix: str, count: int, cursor: str) -> Command:
    return _read_only("PSCAN", prefix, cursor or SCAN_CURSOR_INITIAL, count)


def flushdb() -> Command:
    return _mutating("FLUSHDB")


def truncate() -> Command:
    return _mutating("TRUNCATE")


def inspect(section: InspectSection) -> Command:
    if section == InspectSection.SUMMARY:
        return _read_only("INSPECT")

    return _read_only("INSPECT", section.value)


def stream() -> Command:
    return _read_only("STREAM")


def pstream(prefix: str) -> Command:
    return _read_only("PSTREAM", prefix)


def qstream() -> Command:
    return _read_only("QSTREAM")


def qpstream(prefix: str) -> Command:
    return _read_only("QPSTREAM", prefix)
