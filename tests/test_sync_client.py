import json

import pytest

from fq._reconnect import Deadline, ReconnectPolicy
from fq.errors import AuthError, FQConnectionError, PoolClosedError
from fq.sync.client import Client
from fq.types import (
    CappingKey,
    InspectSection,
    LimitEvent,
    LimitKey,
    QuotaAcquireResult,
    QuotaClientInfo,
    QuotaInfo,
    RateLimitResult,
    ScanKey,
    ScanResult,
)
from tests.fake_server import FakeServer, Session, respond

KEY = CappingKey("k", 60)
LIMIT = LimitKey("k", 60)

FAST = ReconnectPolicy(initial=0.001, max_delay=0.002)


def test_counter_commands_round_trip() -> None:
    handler = respond(
        {
            b"INCR k 60": b"ok|1",
            b"GET k 60": b"ok|7",
            b"WATCH k 60": b"ok|8",
            b"DEL k 60": b"ok|1",
            b"MDEL k 60 j 30": b"ok|1;0",
        }
    )
    with FakeServer(handler) as server, Client(server.address, pool_size=1) as client:
        assert client.incr(KEY) == 1
        assert client.get(KEY) == 7
        assert client.watch(KEY) == 8
        assert client.delete(KEY) is True
        assert client.mdelete([KEY, CappingKey("j", 30)]) == (True, False)


def test_rate_limit_commands_round_trip() -> None:
    handler = respond(
        {
            b"RLIMIT FW k 100 60": b"ok|1;1;99;60",
            b"RLIMIT SW k 100 60": b"ok|1;1;99;60",
            b"RLIMIT TB k 100 10 60": b"ok|0;100;0;6",
        }
    )
    with FakeServer(handler) as server, Client(server.address, pool_size=1) as client:
        assert client.rlimit_fixed_window(LIMIT, 100) == RateLimitResult(True, 1, 99, 60)
        assert client.rlimit_sliding_window(LIMIT, 100) == RateLimitResult(True, 1, 99, 60)
        assert client.rlimit_token_bucket(LIMIT, 100, 10) == RateLimitResult(False, 100, 0, 6)


def test_quota_commands_round_trip() -> None:
    handler = respond(
        {
            b"QUOTA SET plan 10": b"ok|1",
            b"QUOTA SETN plan 10 5": b"ok|1",
            b"QUOTA ACQ plan 4 c1": b"ok|1;4;4;6;0",
            b"QUOTA ACQN plan c1": b"ok|1;2;2;8;0",
            b"QUOTA ACQL plan 10 4 c1 30": b"ok|1;4;4;6;30",
            b"QUOTA REL plan c1": b"ok|1",
            b"QUOTA DEL plan": b"ok|1",
            b"QUOTA INF plan": b"ok|10;4;6;c1;4;0",
        }
    )
    with FakeServer(handler) as server, Client(server.address, pool_size=1) as client:
        assert client.quota_set("plan", 10) is True
        assert client.quota_set_n("plan", 10, 5) is True
        assert client.quota_acquire("plan", 4, "c1") == QuotaAcquireResult(True, 4, 4, 6, 0)
        assert client.quota_acquire_n("plan", "c1").allocated == 2
        assert client.quota_acquire_lease("plan", 10, 4, "c1", ttl=30).expires_after == 30
        assert client.quota_release("plan", "c1") is True
        assert client.quota_delete("plan") is True
        assert client.quota_info("plan") == QuotaInfo(10, 4, 6, (QuotaClientInfo("c1", 4, 0),))


def test_scan_and_maintenance_commands_round_trip() -> None:
    handler = respond(
        {
            b"SCAN 0 100": b"ok|17;a;60",
            b"PSCAN user_ 0 100": b"ok|0;user_1;60",
            b"FLUSHDB": b"ok|1",
            b"TRUNCATE": b"ok|1",
        }
    )
    with FakeServer(handler) as server, Client(server.address, pool_size=1) as client:
        assert client.scan(100) == ScanResult("17", (ScanKey("a", 60),))
        assert client.pscan("user_", 100) == ScanResult("0", (ScanKey("user_1", 60),))
        assert client.flushdb() is True
        assert client.truncate() is True


def test_inspect_is_assembled_from_chunks() -> None:
    report = json.dumps({"section": "WAL", "ts": 1, "wal": {"enabled": True}}).encode()

    def handler(session: Session, request: bytes) -> None:
        session.send(b"nxt|" + report[:10])
        session.send(b"ok|" + report[10:])

    with FakeServer(handler) as server, Client(server.address, pool_size=1) as client:
        result = client.inspect(InspectSection.WAL)

        assert result.wal is not None
        assert result.wal.enabled is True


def test_stream_yields_parsed_events() -> None:
    def handler(session: Session, request: bytes) -> None:
        session.send(b"ok|a;60;1;5")
        session.send(b"ok|b;60;2;4")

    with FakeServer(handler) as server, Client(server.address, pool_size=1) as client:
        events = []
        for event in client.stream():
            events.append(event)
            if len(events) == 2:
                break

        assert events == [LimitEvent("a", 60, 1, 5), LimitEvent("b", 60, 2, 4)]


def test_stream_survives_a_dropped_connection() -> None:
    state = {"dropped": False}

    def handler(session: Session, request: bytes) -> None:
        if not state["dropped"]:
            state["dropped"] = True
            session.send(b"ok|a;60;1;5")
            session.close()

            return

        session.send(b"ok|b;60;2;4")

    with FakeServer(handler) as server:
        with Client(server.address, pool_size=1, reconnect=FAST) as client:
            events = []
            for event in client.pstream("a"):
                events.append(event)
                if len(events) == 2:
                    break

            assert events == [LimitEvent("a", 60, 1, 5), LimitEvent("b", 60, 2, 4)]

        assert server.connections == 2


def test_read_only_command_is_retried_after_the_server_hangs_up() -> None:
    state = {"first": True}

    def handler(session: Session, request: bytes) -> None:
        if state["first"]:
            state["first"] = False
            session.close()

            return

        session.send(b"ok|7")

    with FakeServer(handler) as server:
        with Client(server.address, pool_size=1, reconnect=FAST) as client:
            assert client.get(KEY) == 7

        assert server.connections == 2


def test_mutating_command_is_not_retried_once_the_request_was_sent() -> None:
    def handler(session: Session, request: bytes) -> None:
        session.close()

    with FakeServer(handler) as server:
        with (
            Client(server.address, pool_size=1, reconnect=FAST) as client,
            pytest.raises(FQConnectionError),
        ):
            client.incr(KEY)

        assert server.requests.count(b"INCR k 60") == 1


def test_mutating_command_is_retried_when_the_socket_died_before_the_write() -> None:
    with (
        FakeServer(respond({b"INCR k 60": b"ok|1"})) as server,
        Client(server.address, pool_size=1, reconnect=FAST) as client,
    ):
        with client._pool.acquire(Deadline(1.0)) as connection:
            sock = connection._sock
            assert sock is not None
            sock.close()

        assert client.incr(KEY) == 1
        assert server.requests.count(b"INCR k 60") == 1


def test_role_is_reported_after_the_handshake() -> None:
    with (
        FakeServer(respond({}), token="t", role="ro") as server,
        Client(server.address, pool_size=1, token="t") as client,
    ):
        assert server.requests[0] == b"HELLO 1 AUTH t"
        assert client.server_role == "ro"
        assert client.auth_required is True


def test_permission_denied_surfaces_as_an_auth_error() -> None:
    handler = respond({b"INCR k 60": b"err|3001|permission denied"})
    with FakeServer(handler) as server, Client(server.address, pool_size=1) as client:
        with pytest.raises(AuthError) as excinfo:
            client.incr(KEY)

        assert excinfo.value.code == 3001


def test_closed_client_refuses_further_commands() -> None:
    with FakeServer(respond({})) as server:
        client = Client(server.address, pool_size=1)
        client.close()
        with pytest.raises(PoolClosedError):
            client.get(KEY)


def test_per_call_timeout_overrides_the_client_default() -> None:
    def handler(session: Session, request: bytes) -> None:
        return

    with FakeServer(handler) as server:
        client = Client(server.address, pool_size=1)
        with pytest.raises(FQConnectionError):
            client.get(KEY, timeout=0.05)
        client.close()
