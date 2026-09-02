import json
from contextlib import aclosing

import pytest

from fq._reconnect import ReconnectPolicy
from fq.aio.client import AsyncClient
from fq.errors import AuthError, FQConnectionError, PoolClosedError
from fq.types import (
    CappingKey,
    InspectSection,
    LimitEvent,
    LimitKey,
    QuotaAcquireResult,
    RateLimitResult,
    ScanKey,
    ScanResult,
)
from tests.fake_server import FakeServer, Session, respond

KEY = CappingKey("k", 60)
LIMIT = LimitKey("k", 60)
FAST = ReconnectPolicy(initial=0.001, max_delay=0.002)


async def test_counter_commands_round_trip() -> None:
    handler = respond(
        {
            b"INCR k 60": b"ok|1",
            b"GET k 60": b"ok|7",
            b"DEL k 60": b"ok|1",
            b"MDEL k 60 j 30": b"ok|1;0",
        }
    )
    with FakeServer(handler) as server:
        async with AsyncClient(server.address, pool_size=1, reconnect=FAST) as client:
            assert await client.incr(KEY) == 1
            assert await client.get(KEY) == 7
            assert await client.delete(KEY) is True
            assert await client.mdelete([KEY, CappingKey("j", 30)]) == (True, False)


async def test_connect_classmethod_opens_the_pool() -> None:
    with FakeServer(respond({b"GET k 60": b"ok|7"})) as server:
        client = await AsyncClient.connect(server.address, pool_size=2, reconnect=FAST)

        assert await client.get(KEY) == 7
        assert server.connections == 2
        await client.aclose()


async def test_rate_limit_and_quota_commands_round_trip() -> None:
    handler = respond(
        {
            b"RLIMIT FW k 100 60": b"ok|1;1;99;60",
            b"QUOTA ACQ plan 4 c1": b"ok|1;4;4;6;0",
            b"SCAN 0 10": b"ok|0;a;60",
            b"FLUSHDB": b"ok|1",
        }
    )
    with FakeServer(handler) as server:
        async with AsyncClient(server.address, pool_size=1, reconnect=FAST) as client:
            assert await client.rlimit_fixed_window(LIMIT, 100) == RateLimitResult(
                True, 1, 99, 60
            )
            assert await client.quota_acquire("plan", 4, "c1") == QuotaAcquireResult(
                True, 4, 4, 6, 0
            )
            assert await client.scan(10) == ScanResult("0", (ScanKey("a", 60),))
            assert await client.flushdb() is True


async def test_inspect_is_assembled_from_chunks() -> None:
    report = json.dumps({"section": "", "ts": 5}).encode()

    def handler(session: Session, request: bytes) -> None:
        session.send(b"nxt|" + report[:5])
        session.send(b"ok|" + report[5:])

    with FakeServer(handler) as server:
        async with AsyncClient(server.address, pool_size=1, reconnect=FAST) as client:
            assert (await client.inspect(InspectSection.SUMMARY)).ts == 5


async def test_stream_yields_parsed_events() -> None:
    def handler(session: Session, request: bytes) -> None:
        session.send(b"ok|a;60;1;5")
        session.send(b"ok|b;60;2;4")

    with FakeServer(handler) as server:
        async with AsyncClient(server.address, pool_size=1, reconnect=FAST) as client:
            events = []
            async with aclosing(client.stream()) as stream:
                async for event in stream:
                    events.append(event)
                    if len(events) == 2:
                        break

            assert events == [LimitEvent("a", 60, 1, 5), LimitEvent("b", 60, 2, 4)]


async def test_stream_survives_a_dropped_connection() -> None:
    state = {"dropped": False}

    def handler(session: Session, request: bytes) -> None:
        if not state["dropped"]:
            state["dropped"] = True
            session.send(b"ok|a;60;1;5")
            session.close()

            return

        session.send(b"ok|b;60;2;4")

    with FakeServer(handler) as server:
        async with AsyncClient(server.address, pool_size=1, reconnect=FAST) as client:
            events = []
            async with aclosing(client.stream()) as stream:
                async for event in stream:
                    events.append(event)
                    if len(events) == 2:
                        break

            assert [event.key for event in events] == ["a", "b"]


async def test_read_only_command_is_retried_after_the_server_hangs_up() -> None:
    state = {"first": True}

    def handler(session: Session, request: bytes) -> None:
        if state["first"]:
            state["first"] = False
            session.close()

            return

        session.send(b"ok|7")

    with FakeServer(handler) as server:
        async with AsyncClient(server.address, pool_size=1, reconnect=FAST) as client:
            assert await client.get(KEY) == 7
            assert server.connections == 2


async def test_mutating_command_is_not_retried_once_the_request_was_sent() -> None:
    def handler(session: Session, request: bytes) -> None:
        session.close()

    with FakeServer(handler) as server:
        async with AsyncClient(server.address, pool_size=1, reconnect=FAST) as client:
            with pytest.raises(FQConnectionError):
                await client.incr(KEY)

            assert server.requests.count(b"INCR k 60") == 1


async def test_permission_denied_surfaces_as_an_auth_error() -> None:
    handler = respond({b"INCR k 60": b"err|3001|permission denied"})
    with FakeServer(handler) as server:
        async with AsyncClient(server.address, pool_size=1, reconnect=FAST) as client:
            with pytest.raises(AuthError):
                await client.incr(KEY)


async def test_closed_client_refuses_further_commands() -> None:
    with FakeServer(respond({})) as server:
        client = await AsyncClient.connect(server.address, pool_size=1, reconnect=FAST)
        await client.aclose()

        with pytest.raises(PoolClosedError):
            await client.get(KEY)
