import pytest

from fq._reconnect import ReconnectPolicy
from fq.errors import FQConnectionError, ShardIndexError
from fq.sync.sharded import ShardedClient
from fq.types import CappingKey, InspectSection, ScanKey
from tests.fake_server import FakeServer, Session, respond

FAST = ReconnectPolicy(initial=0.001, max_delay=0.002)


def by_first_letter(key: str, shard_count: int) -> int:
    return 0 if key.startswith("a") else 1


def test_keys_are_routed_by_the_sharding_function() -> None:
    first = respond({b"INCR a 60": b"ok|1"})
    second = respond({b"INCR b 60": b"ok|2"})

    with (
        FakeServer(first) as one,
        FakeServer(second) as two,
        ShardedClient(
            [one.address, two.address],
            sharding_func=by_first_letter,
            pool_size=1,
            reconnect=FAST,
        ) as client,
    ):
        assert client.incr(CappingKey("a", 60)) == 1
        assert client.incr(CappingKey("b", 60)) == 2
        assert one.requests[-1] == b"INCR a 60"
        assert two.requests[-1] == b"INCR b 60"


def test_quotas_are_routed_by_quota_name() -> None:
    first = respond({b"QUOTA INF alpha": b"ok|10;0;10"})
    second = respond({b"QUOTA INF beta": b"ok|20;0;20"})

    with (
        FakeServer(first) as one,
        FakeServer(second) as two,
        ShardedClient(
            [one.address, two.address],
            sharding_func=by_first_letter,
            pool_size=1,
            reconnect=FAST,
        ) as client,
    ):
        assert client.quota_info("alpha").limit == 10
        assert client.quota_info("beta").limit == 20


def test_mdelete_is_split_and_reassembled_in_input_order() -> None:
    first = respond({b"MDEL a 60 aa 60": b"ok|1;0"})
    second = respond({b"MDEL b 60": b"ok|1"})

    with (
        FakeServer(first) as one,
        FakeServer(second) as two,
        ShardedClient(
            [one.address, two.address],
            sharding_func=by_first_letter,
            pool_size=1,
            reconnect=FAST,
        ) as client,
    ):
        keys = [CappingKey("a", 60), CappingKey("b", 60), CappingKey("aa", 60)]

        assert client.mdelete(keys) == (True, True, False)


def test_flushdb_fans_out_and_ands_the_results() -> None:
    with (
        FakeServer(respond({b"FLUSHDB": b"ok|1"})) as one,
        FakeServer(respond({b"FLUSHDB": b"ok|0"})) as two,
        ShardedClient([one.address, two.address], pool_size=1, reconnect=FAST) as client,
    ):
        assert client.flushdb() is False
        assert one.requests.count(b"FLUSHDB") == 1
        assert two.requests.count(b"FLUSHDB") == 1


def test_inspect_returns_one_report_per_shard() -> None:
    def handler(session: Session, request: bytes) -> None:
        session.send(b'ok|{"section":"","ts":1}')

    with (
        FakeServer(handler) as one,
        FakeServer(handler) as two,
        ShardedClient([one.address, two.address], pool_size=1, reconnect=FAST) as client,
    ):
        reports = client.inspect(InspectSection.SUMMARY)

        assert len(reports) == 2
        assert reports[0].ts == 1


def test_scan_walks_shards_with_a_composite_cursor() -> None:
    first = respond({b"SCAN 0 10": b"ok|0;a;60"})
    second = respond({b"SCAN 0 9": b"ok|7;b;60", b"SCAN 7 10": b"ok|0;c;60"})

    with (
        FakeServer(first) as one,
        FakeServer(second) as two,
        ShardedClient([one.address, two.address], pool_size=1, reconnect=FAST) as client,
    ):
        page = client.scan(10)

        assert page.keys == (ScanKey("a", 60), ScanKey("b", 60))
        assert page.cursor == "1:7"

        rest = client.scan(10, page.cursor)

        assert rest.keys == (ScanKey("c", 60),)
        assert rest.cursor == "0"


def test_streams_are_merged_from_every_shard() -> None:
    def first(session: Session, request: bytes) -> None:
        session.send(b"ok|a;60;1;5")

    def second(session: Session, request: bytes) -> None:
        session.send(b"ok|b;60;2;4")

    with (
        FakeServer(first) as one,
        FakeServer(second) as two,
        ShardedClient([one.address, two.address], pool_size=1, reconnect=FAST) as client,
    ):
        events = []
        for event in client.stream():
            events.append(event)
            if len(events) == 2:
                break

        assert sorted(event.key for event in events) == ["a", "b"]


def test_shard_count_and_direct_access() -> None:
    with (
        FakeServer(respond({b"GET k 60": b"ok|3"})) as one,
        FakeServer(respond({})) as two,
        ShardedClient([one.address, two.address], pool_size=1, reconnect=FAST) as client,
    ):
        assert client.shard_count == 2
        assert client.shard(0).get(CappingKey("k", 60)) == 3

        with pytest.raises(ShardIndexError):
            client.shard(3)


def test_out_of_range_shard_index_from_the_sharding_function_is_rejected() -> None:
    with (
        FakeServer(respond({})) as one,
        ShardedClient(
            [one.address],
            sharding_func=lambda key, count: 7,
            pool_size=1,
            reconnect=FAST,
        ) as client,
        pytest.raises(ShardIndexError),
    ):
        client.incr(CappingKey("k", 60))


def test_a_failing_shard_aborts_construction_and_closes_the_rest() -> None:
    with (
        FakeServer(respond({})) as one,
        pytest.raises(FQConnectionError, match="cannot connect"),
    ):
        ShardedClient([one.address, "127.0.0.1:1"], pool_size=1, reconnect=FAST)


def test_incrby_is_routed_by_key() -> None:
    with (
        FakeServer(respond({b"INCRBY a 60 4": b"ok|5"})) as one,
        FakeServer(respond({})) as two,
        ShardedClient(
            [one.address, two.address],
            sharding_func=by_first_letter,
            pool_size=1,
            reconnect=FAST,
        ) as client,
    ):
        assert client.incrby(CappingKey("a", 60), 4) == 5
