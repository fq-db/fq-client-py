"""Tests against a real fq server."""

from __future__ import annotations

import threading
import time
from collections.abc import Iterator

import pytest

from fq.aio.client import AsyncClient
from fq.errors import AuthenticationFailedError, AuthError
from fq.sync.client import Client
from fq.sync.sharded import ShardedClient
from fq.types import CappingKey, InspectSection, LimitKey
from tests.integration.conftest import FQInstance, requires_docker

pytestmark = [pytest.mark.integration, requires_docker]


@pytest.fixture
def client(fq_container: FQInstance) -> Iterator[Client]:
    with Client(fq_container.address, pool_size=2, token=fq_container.admin_token) as client:
        client.flushdb()

        yield client


def test_counters(client: Client) -> None:
    key = CappingKey("integration-counter", 60)

    assert client.incr(key) == 1
    assert client.incr(key) == 2
    assert client.get(key) == 2
    assert client.delete(key) is True
    assert client.get(key) == 0


def test_mdelete_reports_each_key(client: Client) -> None:
    first = CappingKey("mdel-a", 60)
    second = CappingKey("mdel-b", 60)
    client.incr(first)

    assert client.mdelete([first, second]) == (True, False)


def test_fixed_window_rate_limit(client: Client) -> None:
    key = LimitKey("integration-fw", 60)

    assert client.rlimit_fixed_window(key, 2).allowed is True
    assert client.rlimit_fixed_window(key, 2).allowed is True
    assert client.rlimit_fixed_window(key, 2).allowed is False


def test_sliding_window_rate_limit(client: Client) -> None:
    key = LimitKey("integration-sw", 60)

    assert client.rlimit_sliding_window(key, 1).allowed is True
    assert client.rlimit_sliding_window(key, 1).allowed is False


def test_token_bucket_rate_limit(client: Client) -> None:
    key = LimitKey("integration-tb", 60)

    assert client.rlimit_token_bucket(key, 2, 1).allowed is True
    assert client.rlimit_token_bucket(key, 2, 1).allowed is True
    assert client.rlimit_token_bucket(key, 2, 1).allowed is False


def test_server_owned_quota(client: Client) -> None:
    assert client.quota_set("srv-quota", 10) is True

    acquired = client.quota_acquire("srv-quota", 4, "client-a")

    assert acquired.acquired is True
    assert acquired.allocated == 4

    info = client.quota_info("srv-quota")

    assert info.limit == 10
    assert info.used == 4
    assert info.clients[0].client_id == "client-a"

    assert client.quota_release("srv-quota", "client-a") is True
    assert client.quota_delete("srv-quota") is True


def test_client_owned_lease_quota(client: Client) -> None:
    result = client.quota_acquire_lease("lease-quota", 10, 4, "client-a")

    assert result.acquired is True
    assert client.quota_release("lease-quota", "client-a") is True


def test_server_decided_quota(client: Client) -> None:
    assert client.quota_set_n("split-quota", 10, 2) is True

    result = client.quota_acquire_n("split-quota", "client-a")

    assert result.acquired is True
    assert result.allocated == 5


def test_scan_walks_every_key(client: Client) -> None:
    prefix = "scan-test-"
    for index in range(3):
        client.incr(CappingKey(f"{prefix}{index}", 60))

    collected: set[str] = set()
    cursor = "0"
    for _ in range(10):
        page = client.pscan(prefix, 1, cursor)
        collected.update(key.key for key in page.keys)
        cursor = page.cursor
        if cursor == "0":
            break

    assert collected == {f"{prefix}{index}" for index in range(3)}


def test_inspect_reports_the_instance(client: Client) -> None:
    report = client.inspect(InspectSection.ALL)

    assert report.instance is not None
    assert report.instance.role != ""
    assert report.engine is not None
    assert report.engine.key_index_enabled is True


def test_stream_delivers_limit_events(client: Client, fq_container: FQInstance) -> None:
    key = LimitKey("stream-key", 60)

    def produce() -> None:
        time.sleep(0.5)
        with Client(
            fq_container.address,
            pool_size=1,
            token=fq_container.admin_token,
        ) as producer:
            producer.rlimit_fixed_window(key, 1)
            producer.rlimit_fixed_window(key, 1)

    producer_thread = threading.Thread(target=produce, daemon=True)
    producer_thread.start()

    give_up_at = time.monotonic() + 15
    events = []
    for event in client.stream_polled(lambda: time.monotonic() > give_up_at):
        events.append(event)

        break

    producer_thread.join(timeout=5)

    assert events
    assert events[0].key == "stream-key"


def test_read_only_token_cannot_write(fq_container: FQInstance) -> None:
    with Client(fq_container.address, pool_size=1, token=fq_container.ro_token) as client:
        assert client.server_role == "ro"

        with pytest.raises(AuthError) as excinfo:
            client.incr(CappingKey("ro-key", 60))

        assert excinfo.value.code == 3001


def test_wrong_token_is_rejected_at_construction(fq_container: FQInstance) -> None:
    with pytest.raises(AuthenticationFailedError):
        Client(fq_container.address, pool_size=1, token="definitely-wrong")


async def test_async_client_against_the_real_server(fq_container: FQInstance) -> None:
    async with AsyncClient(
        fq_container.address,
        pool_size=2,
        token=fq_container.admin_token,
    ) as client:
        key = CappingKey("async-counter", 60)
        await client.flushdb()

        assert await client.incr(key) == 1
        assert await client.get(key) == 1
        assert (await client.inspect(InspectSection.SUMMARY)).instance is not None


def test_sharded_client_spreads_keys(fq_cluster: list[FQInstance]) -> None:
    addresses = [instance.address for instance in fq_cluster]
    with ShardedClient(addresses, pool_size=1, token=fq_cluster[0].admin_token) as client:
        client.flushdb()

        for index in range(20):
            assert client.incr(CappingKey(f"sharded-{index}", 60)) == 1

        page = client.scan(100)

        assert len(page.keys) == 20
