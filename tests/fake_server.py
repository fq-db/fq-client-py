"""A stub server that speaks the fq protocol from a test script."""

from __future__ import annotations

import contextlib
import socket
import ssl
import threading
from collections.abc import Callable

from fq._framing import FRAME_HEADER_SIZE, decode_frame_size, encode_frame


class Session:
    """One client connection as seen by the stub server."""

    def __init__(self, sock: socket.socket, max_message_size: int) -> None:
        self._sock = sock
        self._max_message_size = max_message_size

    def read_request(self) -> bytes | None:
        """Read one frame; ``None`` when the client closed the connection."""
        header = self._read_exactly(FRAME_HEADER_SIZE)
        if header is None:
            return None

        size = decode_frame_size(header, self._max_message_size)
        if size == 0:
            return b""

        return self._read_exactly(size)

    def send(self, payload: bytes) -> None:
        """Send a payload as one frame."""
        self._sock.sendall(encode_frame(payload))

    def send_raw(self, data: bytes) -> None:
        """Send bytes verbatim, for tests of malformed frames."""
        self._sock.sendall(data)

    def close(self) -> None:
        """Drop the connection."""
        with contextlib.suppress(OSError):
            self._sock.shutdown(socket.SHUT_RDWR)

        self._sock.close()

    def _read_exactly(self, size: int) -> bytes | None:
        buffer = bytearray()
        while len(buffer) < size:
            chunk = self._sock.recv(size - len(buffer))
            if not chunk:
                return None
            buffer.extend(chunk)

        return bytes(buffer)


_ACCEPT_POLL_INTERVAL = 0.01

Handler = Callable[[Session, bytes], None]


def respond(mapping: dict[bytes, bytes]) -> Handler:
    """A table-driven handler: request to response."""

    def handler(session: Session, request: bytes) -> None:
        session.send(mapping.get(request, b"err|1001|invalid command"))

    return handler


class FakeServer:
    """A thread-per-connection TCP server answering from a script.

    ``HELLO`` is handled by the server itself: it negotiates the version, frame
    size, auth flag and role. Everything else goes to the handler.
    """

    def __init__(
        self,
        handler: Handler,
        *,
        ssl_context: ssl.SSLContext | None = None,
        max_message_size: int = 4096,
        token: str | None = None,
        role: str = "admin",
    ) -> None:
        self._handler = handler
        self._ssl_context = ssl_context
        self._max_message_size = max_message_size
        self._token = token
        self._role = role

        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind(("127.0.0.1", 0))
        self._sock.listen(16)
        # Closing a listening socket does not wake a thread blocked in accept()
        # on Linux, which would leave the port bound after stop() returns.
        self._sock.settimeout(_ACCEPT_POLL_INTERVAL)

        host, port = self._sock.getsockname()
        self.address = f"{host}:{port}"
        self.requests: list[bytes] = []
        self.connections = 0

        self._threads: list[threading.Thread] = []
        self._running = True
        self._accept_thread = threading.Thread(target=self._accept_loop, daemon=True)

    def __enter__(self) -> FakeServer:
        self._accept_thread.start()

        return self

    def __exit__(self, *exc_info: object) -> None:
        self.stop()

    def stop(self) -> None:
        self._running = False
        if self._accept_thread.is_alive():
            self._wake_accept()
            self._accept_thread.join(timeout=2.0)

        self._sock.close()
        for thread in self._threads:
            thread.join(timeout=2.0)

    def _wake_accept(self) -> None:
        """Nudge the accept loop so it sees the stop flag without waiting a tick."""
        with contextlib.suppress(OSError):
            host, port = self._sock.getsockname()
            with socket.create_connection((host, port), timeout=1.0):
                pass

    def _accept_loop(self) -> None:
        while self._running:
            try:
                client, _ = self._sock.accept()
            except TimeoutError:
                continue
            except OSError:
                return

            if not self._running:
                client.close()

                return

            client.settimeout(None)
            self.connections += 1
            thread = threading.Thread(target=self._serve, args=(client,), daemon=True)
            self._threads.append(thread)
            thread.start()

    def _serve(self, client: socket.socket) -> None:
        if self._ssl_context is not None:
            try:
                client = self._ssl_context.wrap_socket(client, server_side=True)
            except OSError:
                return

        session = Session(client, self._max_message_size)
        try:
            while True:
                request = session.read_request()
                if request is None:
                    return

                self.requests.append(request)

                if request.startswith(b"HELLO"):
                    self._handle_hello(session, request)

                    continue

                self._handler(session, request)
        except OSError:
            return
        finally:
            with contextlib.suppress(OSError):
                client.close()

    def _handle_hello(self, session: Session, request: bytes) -> None:
        auth_required = b"1" if self._token else b"0"

        if self._token is not None:
            marker = b" AUTH "
            index = request.find(marker)
            supplied = request[index + len(marker) :] if index != -1 else b""
            if supplied != self._token.encode():
                session.send(b"err|3002|authentication failed")

                return

        session.send(
            b"ok|1;"
            + str(self._max_message_size).encode()
            + b";"
            + auth_required
            + b";"
            + self._role.encode()
        )
