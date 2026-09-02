"""Pure sharded-client logic: routing and cursors."""

from __future__ import annotations

from collections.abc import Callable, Sequence

from fq.errors import CorruptedResponseError, ShardIndexError
from fq.types import SCAN_CURSOR_INITIAL, CappingKey

ShardingFunc = Callable[[str, int], int]

SHARD_CURSOR_SEPARATOR = ":"

_FNV_OFFSET_BASIS = 0x811C9DC5
_FNV_PRIME = 0x01000193
_UINT32_MASK = 0xFFFFFFFF


def fnv1a_shard(key: str, shard_count: int) -> int:
    """The default sharding function: FNV-1a over the key's UTF-8 bytes."""
    digest = _FNV_OFFSET_BASIS
    for byte in key.encode("utf-8"):
        digest = ((digest ^ byte) * _FNV_PRIME) & _UINT32_MASK

    return digest % shard_count


def resolve_shard(key: str, shard_count: int, sharding_func: ShardingFunc) -> int:
    """Compute a shard index and check that it is in range."""
    index = sharding_func(key, shard_count)
    if index < 0 or index >= shard_count:
        raise ShardIndexError(f"sharding function returned {index} for {shard_count} shards")

    return index


def group_by_shard(
    keys: Sequence[CappingKey],
    resolve: Callable[[str], int],
) -> dict[int, list[tuple[int, CappingKey]]]:
    """Group keys by shard, keeping their original positions."""
    grouped: dict[int, list[tuple[int, CappingKey]]] = {}
    for position, key in enumerate(keys):
        grouped.setdefault(resolve(key.key), []).append((position, key))

    return grouped


def parse_shard_cursor(cursor: str, shard_count: int) -> tuple[int, str]:
    """Parse a composite ``"<shard>:<cursor>"`` cursor."""
    if cursor in ("", SCAN_CURSOR_INITIAL):
        return 0, SCAN_CURSOR_INITIAL

    shard, separator, shard_cursor = cursor.partition(SHARD_CURSOR_SEPARATOR)
    if not separator or not shard_cursor or not shard.isdigit():
        raise CorruptedResponseError(f"malformed shard cursor: {cursor!r}")

    index = int(shard)
    if index >= shard_count:
        raise CorruptedResponseError(f"shard cursor points at shard {index} of {shard_count}")

    return index, shard_cursor


def make_shard_cursor(shard_index: int, shard_cursor: str, shard_count: int) -> str:
    """Compose a cursor; past the last shard the walk is finished."""
    if shard_index >= shard_count:
        return SCAN_CURSOR_INITIAL

    return f"{shard_index}{SHARD_CURSOR_SEPARATOR}{shard_cursor}"
