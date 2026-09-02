import ssl

import pytest

from fq._commands import get, incr
from fq._reconnect import Deadline
from fq.errors import (
    AuthenticationFailedError,
    FQConnectionError,
    FQTimeoutError,
    RequestError,
)
from fq.sync.connection import Connection, split_address
from fq.types import CappingKey, TLSConfig
from tests.fake_server import FakeServer, Session, respond

KEY = CappingKey("k", 60)


def drain(connection: Connection, frames: list[bytes | None]) -> None:
    for frame in connection.open_stream(get(KEY)):
        frames.append(frame)


def open_connection(server: FakeServer, *, token: str | None = None) -> Connection:
    connection = Connection(server.address, token=token)
    connection.connect()

    return connection


def test_address_is_split_into_host_and_port() -> None:
    assert split_address("127.0.0.1:1945") == ("127.0.0.1", 1945)
    assert split_address("fq.internal:1945") == ("fq.internal", 1945)


def test_handshake_negotiates_max_message_size_and_role() -> None:
    with FakeServer(respond({}), max_message_size=65536, role="rw") as server:
        connection = open_connection(server)

        assert connection.max_message_size == 65536
        assert connection.role == "rw"
        assert connection.auth_required is False
        connection.close()


def test_token_is_sent_inline_with_hello() -> None:
    with FakeServer(respond({}), token="s3cret") as server:
        connection = open_connection(server, token="s3cret")

        assert server.requests[0] == b"HELLO 1 AUTH s3cret"
        assert connection.auth_required is True
        connection.close()


def test_rejected_token_fails_the_connection() -> None:
    with FakeServer(respond({}), token="right") as server:
        connection = Connection(server.address, token="wrong")

        with pytest.raises(AuthenticationFailedError):
            connection.connect()


def test_request_and_response_round_trip() -> None:
    with FakeServer(respond({b"GET k 60": b"ok|7"})) as server:
        connection = open_connection(server)

        assert connection.send(get(KEY), Deadline(1.0)) == b"ok|7"
        assert connection.request_in_flight is False
        connection.close()


def test_request_in_flight_is_set_when_the_server_never_answers() -> None:
    def never_answer(session: Session, request: bytes) -> None:
        return

    with FakeServer(never_answer) as server:
        connection = open_connection(server)

        with pytest.raises(FQTimeoutError):
            connection.send(incr(KEY), Deadline(0.2))

        assert connection.request_in_flight is True
        connection.close()


def test_closed_connection_surfaces_as_a_connection_error() -> None:
    def hang_up(session: Session, request: bytes) -> None:
        session.close()

    with FakeServer(hang_up) as server:
        connection = open_connection(server)

        with pytest.raises(FQConnectionError):
            connection.send(get(KEY), Deadline(1.0))

        connection.close()


def test_reconnect_repeats_the_handshake() -> None:
    with FakeServer(respond({b"GET k 60": b"ok|7"}), token="s3cret") as server:
        connection = open_connection(server, token="s3cret")
        connection.reconnect()

        assert connection.send(get(KEY), Deadline(1.0)) == b"ok|7"
        assert server.requests.count(b"HELLO 1 AUTH s3cret") == 2
        assert server.connections == 2
        connection.close()


def test_chunked_response_is_assembled() -> None:
    def chunked(session: Session, request: bytes) -> None:
        session.send(b"nxt|{'a':")
        session.send(b"nxt|1")
        session.send(b"ok|}")

    with FakeServer(chunked) as server:
        connection = open_connection(server)

        assert connection.send_chunked(get(KEY), Deadline(1.0)) == b"ok|{'a':1}"
        connection.close()


def test_stream_yields_every_frame() -> None:
    def stream_events(session: Session, request: bytes) -> None:
        session.send(b"ok|a;60;1;5")
        session.send(b"ok|b;60;2;4")
        session.close()

    with FakeServer(stream_events) as server:
        connection = open_connection(server)
        frames: list[bytes | None] = []

        with pytest.raises(FQConnectionError):
            drain(connection, frames)

        assert frames == [b"ok|a;60;1;5", b"ok|b;60;2;4"]
        connection.close()


def test_poll_interval_yields_idle_ticks() -> None:
    def silent(session: Session, request: bytes) -> None:
        return

    with FakeServer(silent) as server:
        connection = open_connection(server)

        for frame in connection.open_stream(get(KEY), poll_interval=0.05):
            assert frame is None

            break

        connection.close()


def test_oversized_request_is_refused_before_sending() -> None:
    with FakeServer(respond({}), max_message_size=16) as server:
        connection = open_connection(server)
        big = CappingKey("x" * 100, 60)

        with pytest.raises(RequestError, match="exceeds max message size"):
            connection.send(get(big), Deadline(1.0))

        connection.close()


def test_tls_connection_completes_the_handshake(
    server_ssl_context: ssl.SSLContext,
    client_tls_config: TLSConfig,
) -> None:
    with FakeServer(respond({b"GET k 60": b"ok|7"}), ssl_context=server_ssl_context) as server:
        connection = Connection(
            server.address,
            ssl_context=client_tls_config.build_context(),
            server_hostname="localhost",
        )
        connection.connect()

        assert connection.send(get(KEY), Deadline(2.0)) == b"ok|7"
        connection.close()


def test_connecting_to_a_dead_port_raises_a_connection_error() -> None:
    with FakeServer(respond({})) as server:
        address = server.address
    connection = Connection(address, connect_timeout=1.0)

    with pytest.raises(FQConnectionError):
        connection.connect()
