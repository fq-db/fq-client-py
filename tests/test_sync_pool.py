import pytest

from fq._commands import get
from fq._reconnect import Deadline
from fq.errors import FQTimeoutError, PoolClosedError
from fq.sync.connection import Connection
from fq.sync.pool import ConnectionPool
from fq.types import CappingKey
from tests.fake_server import FakeServer, respond

KEY = CappingKey("k", 60)


def make_pool(server: FakeServer, size: int) -> ConnectionPool:
    def factory() -> Connection:
        connection = Connection(server.address)
        connection.connect()

        return connection

    return ConnectionPool(size, factory)


def test_prefill_opens_every_connection_upfront() -> None:
    with FakeServer(respond({})) as server:
        pool = make_pool(server, 3)
        pool.prefill()

        assert server.connections == 3
        pool.close()


def test_connection_is_reused_after_release() -> None:
    with FakeServer(respond({b"GET k 60": b"ok|7"})) as server:
        pool = make_pool(server, 1)
        pool.prefill()

        for _ in range(3):
            with pool.acquire(Deadline(1.0)) as connection:
                assert connection.send(get(KEY), Deadline(1.0)) == b"ok|7"

        assert server.connections == 1
        pool.close()


def test_exhausted_pool_times_out() -> None:
    with FakeServer(respond({})) as server:
        pool = make_pool(server, 1)
        pool.prefill()

        with (
            pool.acquire(Deadline(1.0)),
            pytest.raises(FQTimeoutError),
            pool.acquire(Deadline(0.1)),
        ):
            pass

        pool.close()


def test_dead_connection_frees_its_slot() -> None:
    with FakeServer(respond({})) as server:
        pool = make_pool(server, 1)
        pool.prefill()

        with pool.acquire(Deadline(1.0)) as connection:
            connection.close()

        with pool.acquire(Deadline(1.0)) as connection:
            assert connection.closed is False

        assert server.connections == 2
        pool.close()


def test_closed_pool_refuses_new_work() -> None:
    with FakeServer(respond({})) as server:
        pool = make_pool(server, 1)
        pool.prefill()
        pool.close()

        with pytest.raises(PoolClosedError), pool.acquire(Deadline(1.0)):
            pass


def test_close_is_idempotent() -> None:
    with FakeServer(respond({})) as server:
        pool = make_pool(server, 2)
        pool.prefill()
        pool.close()
        pool.close()
