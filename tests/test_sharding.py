import pytest

from fq._sharding import (
    fnv1a_shard,
    group_by_shard,
    make_shard_cursor,
    parse_shard_cursor,
    resolve_shard,
)
from fq.errors import CorruptedResponseError, ShardIndexError
from fq.types import CappingKey


def test_fnv1a_is_stable_and_in_range() -> None:
    assert fnv1a_shard("user_42", 4) == fnv1a_shard("user_42", 4)
    assert 0 <= fnv1a_shard("user_42", 4) < 4


def test_fnv1a_matches_the_reference_hash() -> None:
    digest = 0x811C9DC5
    for byte in b"user_42":
        digest = ((digest ^ byte) * 0x01000193) & 0xFFFFFFFF

    assert fnv1a_shard("user_42", 2**32 - 1) == digest % (2**32 - 1)


def test_fnv1a_spreads_keys_across_shards() -> None:
    shards = {fnv1a_shard(f"user_{index}", 4) for index in range(100)}

    assert len(shards) == 4


def test_index_out_of_range_is_rejected() -> None:
    with pytest.raises(ShardIndexError):
        resolve_shard("k", 2, lambda key, count: 5)

    with pytest.raises(ShardIndexError):
        resolve_shard("k", 2, lambda key, count: -1)


def test_valid_index_passes_through() -> None:
    assert resolve_shard("k", 2, lambda key, count: 1) == 1


def test_keys_are_grouped_with_their_original_positions() -> None:
    keys = [CappingKey("a", 60), CappingKey("b", 60), CappingKey("c", 60)]
    grouped = group_by_shard(keys, lambda key: 0 if key == "b" else 1)

    assert grouped == {
        1: [(0, CappingKey("a", 60)), (2, CappingKey("c", 60))],
        0: [(1, CappingKey("b", 60))],
    }


def test_initial_cursor_starts_at_the_first_shard() -> None:
    assert parse_shard_cursor("0", 3) == (0, "0")
    assert parse_shard_cursor("", 3) == (0, "0")


def test_composite_cursor_is_parsed() -> None:
    assert parse_shard_cursor("2:abc", 3) == (2, "abc")


def test_cursor_with_an_out_of_range_shard_is_corrupted() -> None:
    with pytest.raises(CorruptedResponseError):
        parse_shard_cursor("9:abc", 3)


def test_malformed_cursor_is_corrupted() -> None:
    with pytest.raises(CorruptedResponseError):
        parse_shard_cursor("abc", 3)

    with pytest.raises(CorruptedResponseError):
        parse_shard_cursor("1:", 3)


def test_cursor_is_composed_from_shard_and_shard_cursor() -> None:
    assert make_shard_cursor(1, "abc", 3) == "1:abc"


def test_cursor_past_the_last_shard_means_the_scan_is_over() -> None:
    assert make_shard_cursor(3, "0", 3) == "0"
