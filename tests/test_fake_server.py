import socket

from fq._framing import FRAME_HEADER_SIZE, decode_frame_size, encode_frame
from tests.fake_server import FakeServer, respond


def read_frame(sock: socket.socket) -> bytes:
    header = sock.recv(FRAME_HEADER_SIZE)
    size = decode_frame_size(header, 4096)

    return sock.recv(size)


def test_server_answers_the_handshake_and_records_requests() -> None:
    with FakeServer(respond({b"GET k 60": b"ok|7"})) as server:
        host, port = server.address.rsplit(":", 1)
        with socket.create_connection((host, int(port))) as sock:
            sock.sendall(encode_frame(b"HELLO 1"))
            assert read_frame(sock) == b"ok|1;4096;0;admin"

            sock.sendall(encode_frame(b"GET k 60"))
            assert read_frame(sock) == b"ok|7"

    assert server.requests == [b"HELLO 1", b"GET k 60"]
    assert server.connections == 1


def test_server_rejects_a_wrong_token() -> None:
    with FakeServer(respond({}), token="right") as server:
        host, port = server.address.rsplit(":", 1)
        with socket.create_connection((host, int(port))) as sock:
            sock.sendall(encode_frame(b"HELLO 1 AUTH wrong"))

            assert read_frame(sock) == b"err|3002|authentication failed"


def test_server_accepts_the_right_token_and_reports_auth_required() -> None:
    with FakeServer(respond({}), token="right", role="rw") as server:
        host, port = server.address.rsplit(":", 1)
        with socket.create_connection((host, int(port))) as sock:
            sock.sendall(encode_frame(b"HELLO 1 AUTH right"))

            assert read_frame(sock) == b"ok|1;4096;1;rw"


def test_unknown_request_gets_an_error_response() -> None:
    with FakeServer(respond({})) as server:
        host, port = server.address.rsplit(":", 1)
        with socket.create_connection((host, int(port))) as sock:
            sock.sendall(encode_frame(b"HELLO 1"))
            read_frame(sock)
            sock.sendall(encode_frame(b"NOPE"))

            assert read_frame(sock) == b"err|1001|invalid command"
