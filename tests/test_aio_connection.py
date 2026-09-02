import ssl

import pytest

from fq._commands import get, incr
from fq._reconnect import Deadline
from fq.aio.connection import AsyncConnection
from fq.errors import AuthenticationFailedError, FQConnectionError, FQTimeoutError
from fq.types import CappingKey, TLSConfig
from tests.fake_server import FakeServer, Session, respond

KEY = CappingKey("k", 60)


async def open_connection(server: FakeServer, *, token: str | None = None) -> AsyncConnection:
    connection = AsyncConnection(server.address, token=token)
    await connection.connect()

    return connection


async def test_handshake_negotiates_max_message_size_and_role() -> None:
    with FakeServer(respond({}), max_message_size=65536, role="rw") as server:
        connection = await open_connection(server)

        assert connection.max_message_size == 65536
        assert connection.role == "rw"
        await connection.close()


async def test_token_is_sent_inline_with_hello() -> None:
    with FakeServer(respond({}), token="s3cret") as server:
        connection = await open_connection(server, token="s3cret")

        assert server.requests[0] == b"HELLO 1 AUTH s3cret"
        assert connection.auth_required is True
        await connection.close()


async def test_rejected_token_fails_the_connection() -> None:
    with FakeServer(respond({}), token="right") as server:
        connection = AsyncConnection(server.address, token="wrong")

        with pytest.raises(AuthenticationFailedError):
            await connection.connect()


async def test_request_and_response_round_trip() -> None:
    with FakeServer(respond({b"GET k 60": b"ok|7"})) as server:
        connection = await open_connection(server)

        assert await connection.send(get(KEY), Deadline(1.0)) == b"ok|7"
        assert connection.request_in_flight is False
        await connection.close()


async def test_request_in_flight_is_set_when_the_server_never_answers() -> None:
    def never_answer(session: Session, request: bytes) -> None:
        return

    with FakeServer(never_answer) as server:
        connection = await open_connection(server)

        with pytest.raises(FQTimeoutError):
            await connection.send(incr(KEY), Deadline(0.2))

        assert connection.request_in_flight is True
        await connection.close()


async def test_closed_connection_surfaces_as_a_connection_error() -> None:
    def hang_up(session: Session, request: bytes) -> None:
        session.close()

    with FakeServer(hang_up) as server:
        connection = await open_connection(server)

        with pytest.raises(FQConnectionError):
            await connection.send(get(KEY), Deadline(1.0))

        await connection.close()


async def test_reconnect_repeats_the_handshake() -> None:
    with FakeServer(respond({b"GET k 60": b"ok|7"}), token="s3cret") as server:
        connection = await open_connection(server, token="s3cret")
        await connection.reconnect()

        assert await connection.send(get(KEY), Deadline(1.0)) == b"ok|7"
        assert server.requests.count(b"HELLO 1 AUTH s3cret") == 2
        await connection.close()


async def test_chunked_response_is_assembled() -> None:
    def chunked(session: Session, request: bytes) -> None:
        session.send(b"nxt|{'a':")
        session.send(b"ok|1}")

    with FakeServer(chunked) as server:
        connection = await open_connection(server)

        assert await connection.send_chunked(get(KEY), Deadline(1.0)) == b"ok|{'a':1}"
        await connection.close()


async def test_stream_yields_every_frame() -> None:
    def stream_events(session: Session, request: bytes) -> None:
        session.send(b"ok|a;60;1;5")
        session.send(b"ok|b;60;2;4")

    with FakeServer(stream_events) as server:
        connection = await open_connection(server)
        frames = []

        async for frame in connection.open_stream(get(KEY)):
            frames.append(frame)
            if len(frames) == 2:
                break

        assert frames == [b"ok|a;60;1;5", b"ok|b;60;2;4"]
        await connection.close()


async def test_poll_interval_yields_idle_ticks() -> None:
    def silent(session: Session, request: bytes) -> None:
        return

    with FakeServer(silent) as server:
        connection = await open_connection(server)

        async for frame in connection.open_stream(get(KEY), poll_interval=0.05):
            assert frame is None

            break

        await connection.close()


async def test_tls_connection_completes_the_handshake(
    server_ssl_context: ssl.SSLContext,
    client_tls_config: TLSConfig,
) -> None:
    with FakeServer(respond({b"GET k 60": b"ok|7"}), ssl_context=server_ssl_context) as server:
        connection = AsyncConnection(
            server.address,
            ssl_context=client_tls_config.build_context(),
            server_hostname="localhost",
        )
        await connection.connect()

        assert await connection.send(get(KEY), Deadline(2.0)) == b"ok|7"
        await connection.close()
