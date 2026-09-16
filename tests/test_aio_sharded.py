from contextlib import aclosing

import pytest

from fq._reconnect import ReconnectPolicy
from fq.aio.sharded import AsyncShardedClient
from fq.errors import ShardIndexError
from fq.types import CappingKey, InspectSection, ScanKey
from tests.fake_server import FakeServer, Session, respond

FAST = ReconnectPolicy(initial=0.001, max_delay=0.002)


def by_first_letter(key: str, shard_count: int) -> int:
    return 0 if key.startswith("a") else 1


async def test_keys_are_routed_by_the_sharding_function() -> None:
    first = respond({b"INCR a 60": b"ok|1"})
    second = respond({b"INCR b 60": b"ok|2"})

    with FakeServer(first) as one, FakeServer(second) as two:
        async with AsyncShardedClient(
            [one.address, two.address],
            sharding_func=by_first_letter,
            pool_size=1,
            reconnect=FAST,
        ) as client:
            assert await client.incr(CappingKey("a", 60)) == 1
            assert await client.incr(CappingKey("b", 60)) == 2


async def test_mdelete_is_split_and_reassembled_in_input_order() -> None:
    first = respond({b"MDEL a 60 aa 60": b"ok|1;0"})
    second = respond({b"MDEL b 60": b"ok|1"})

    with FakeServer(first) as one, FakeServer(second) as two:
        async with AsyncShardedClient(
            [one.address, two.address],
            sharding_func=by_first_letter,
            pool_size=1,
            reconnect=FAST,
        ) as client:
            keys = [CappingKey("a", 60), CappingKey("b", 60), CappingKey("aa", 60)]

            assert await client.mdelete(keys) == (True, True, False)


async def test_truncate_fans_out_and_ands_the_results() -> None:
    with (
        FakeServer(respond({b"TRUNCATE": b"ok|1"})) as one,
        FakeServer(respond({b"TRUNCATE": b"ok|0"})) as two,
    ):
        async with AsyncShardedClient(
            [one.address, two.address],
            pool_size=1,
            reconnect=FAST,
        ) as client:
            assert await client.truncate() is False


async def test_inspect_returns_one_report_per_shard() -> None:
    def handler(session: Session, request: bytes) -> None:
        session.send(b'ok|{"section":"","ts":1}')

    with FakeServer(handler) as one, FakeServer(handler) as two:
        async with AsyncShardedClient(
            [one.address, two.address],
            pool_size=1,
            reconnect=FAST,
        ) as client:
            reports = await client.inspect(InspectSection.SUMMARY)

            assert len(reports) == 2


async def test_scan_walks_shards_with_a_composite_cursor() -> None:
    first = respond({b"SCAN 0 10": b"ok|0;a;60"})
    second = respond({b"SCAN 0 9": b"ok|7;b;60"})

    with FakeServer(first) as one, FakeServer(second) as two:
        async with AsyncShardedClient(
            [one.address, two.address],
            pool_size=1,
            reconnect=FAST,
        ) as client:
            page = await client.scan(10)

            assert page.keys == (ScanKey("a", 60), ScanKey("b", 60))
            assert page.cursor == "1:7"


async def test_streams_are_merged_from_every_shard() -> None:
    def first(session: Session, request: bytes) -> None:
        session.send(b"ok|a;60;1;5")

    def second(session: Session, request: bytes) -> None:
        session.send(b"ok|b;60;2;4")

    with FakeServer(first) as one, FakeServer(second) as two:
        async with AsyncShardedClient(
            [one.address, two.address],
            pool_size=1,
            reconnect=FAST,
        ) as client:
            events = []
            async with aclosing(client.stream()) as stream:
                async for event in stream:
                    events.append(event)
                    if len(events) == 2:
                        break

            assert sorted(event.key for event in events) == ["a", "b"]


async def test_out_of_range_shard_index_is_rejected() -> None:
    with FakeServer(respond({})) as one:
        async with AsyncShardedClient([one.address], pool_size=1, reconnect=FAST) as client:
            with pytest.raises(ShardIndexError):
                client.shard(3)


async def test_incrby_is_routed_by_key() -> None:
    with FakeServer(respond({b"INCRBY a 60 4": b"ok|5"})) as one, FakeServer(respond({})) as two:
        async with AsyncShardedClient(
            [one.address, two.address],
            sharding_func=by_first_letter,
            pool_size=1,
            reconnect=FAST,
        ) as client:
            assert await client.incrby(CappingKey("a", 60), 4) == 5
