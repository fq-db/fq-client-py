"""Synchronous transport: socket, TLS and frame exchange.

This module neither parses nor builds protocol bytes; that lives in the core
(``fq._commands``, ``fq._responses``). Here there is only I/O.
"""

from __future__ import annotations

import socket
import ssl
from collections.abc import Iterator

from fq._chunked import ChunkAssembler
from fq._commands import Command, hello
from fq._framing import FRAME_HEADER_SIZE, check_request_size, decode_frame_size, encode_frame
from fq._reconnect import Deadline
from fq._responses import parse_hello
from fq.errors import CorruptedResponseError, FQConnectionError, FQTimeoutError
from fq.types import DEFAULT_MAX_MESSAGE_SIZE, PROTOCOL_VERSION


def split_address(address: str) -> tuple[str, int]:
    """Split ``"host:port"`` into host and port."""
    host, separator, port = address.rpartition(":")
    if not separator or not port.isdigit():
        raise ValueError(f"address must be in the form host:port, got {address!r}")

    return host, int(port)


class Connection:
    """One connection to an fq server."""

    def __init__(
        self,
        address: str,
        *,
        token: str | None = None,
        ssl_context: ssl.SSLContext | None = None,
        server_hostname: str | None = None,
        connect_timeout: float = 5.0,
    ) -> None:
        self._address = address
        self._token = token
        self._ssl_context = ssl_context
        self._server_hostname = server_hostname
        self._connect_timeout = connect_timeout

        self._sock: socket.socket | None = None
        self.max_message_size = DEFAULT_MAX_MESSAGE_SIZE
        self.role = ""
        self.auth_required = False
        self.request_in_flight = False

    @property
    def closed(self) -> bool:
        return self._sock is None

    def connect(self) -> None:
        """Open the connection and run the handshake."""
        host, port = split_address(self._address)
        try:
            sock = socket.create_connection((host, port), timeout=self._connect_timeout)
        except OSError as error:
            raise FQConnectionError(f"cannot connect to {self._address}: {error}") from error

        if self._ssl_context is not None:
            try:
                sock = self._ssl_context.wrap_socket(
                    sock,
                    server_hostname=self._server_hostname or host,
                )
            except OSError as error:
                sock.close()

                raise FQConnectionError(f"tls handshake failed: {error}") from error

        self._sock = sock
        self.max_message_size = DEFAULT_MAX_MESSAGE_SIZE
        self.request_in_flight = False

        try:
            self._handshake()
        except BaseException:
            self.close()

            raise

    def reconnect(self) -> None:
        """Reopen the connection and renegotiate the protocol."""
        self.close()
        self.connect()

    def close(self) -> None:
        if self._sock is None:
            return

        try:
            self._sock.close()
        except OSError:
            pass
        finally:
            self._sock = None

    def send(self, command: Command, deadline: Deadline) -> bytes:
        """Send a command and read one response frame."""
        self._write_request(command, deadline)
        response = self._read_frame(deadline)
        self.request_in_flight = False

        return response

    def send_chunked(self, command: Command, deadline: Deadline) -> bytes:
        """Send a command and assemble a response spanning several frames."""
        self._write_request(command, deadline)

        assembler = ChunkAssembler()
        while True:
            frame = self._read_frame(deadline)
            response = assembler.feed(frame)
            if response is not None:
                self.request_in_flight = False

                return response

    def open_stream(
        self,
        command: Command,
        poll_interval: float | None = None,
    ) -> Iterator[bytes | None]:
        """Send a command and yield incoming frames forever.

        With ``poll_interval`` set, the generator yields ``None`` whenever no
        frame arrived within that interval. That gives the caller a point to
        check a stop flag without closing the socket from another thread.
        """
        self._write_request(command, Deadline(None))

        while True:
            if poll_interval is None:
                yield self._read_frame(Deadline(None))

                continue

            try:
                yield self._read_frame(Deadline(poll_interval))
            except FQTimeoutError:
                yield None

    def _handshake(self) -> None:
        info = parse_hello(self.send(hello(self._token), Deadline(self._connect_timeout)))
        if info.version != PROTOCOL_VERSION:
            raise CorruptedResponseError(f"server negotiated version {info.version}")

        self.max_message_size = info.max_message_size
        self.auth_required = info.auth_required
        self.role = info.role

    def _write_request(self, command: Command, deadline: Deadline) -> None:
        check_request_size(command.payload, self.max_message_size)
        sock = self._require_socket()
        self._apply_timeout(sock, deadline)

        try:
            sock.sendall(encode_frame(command.payload))
        except TimeoutError as error:
            self.request_in_flight = True

            raise FQTimeoutError(f"deadline exceeded while sending: {error}") from error
        except OSError as error:
            raise FQConnectionError(f"connection lost while sending: {error}") from error

        self.request_in_flight = True

    def _read_frame(self, deadline: Deadline) -> bytes:
        sock = self._require_socket()
        self._apply_timeout(sock, deadline)

        header = self._read_exactly(sock, FRAME_HEADER_SIZE)
        size = decode_frame_size(header, self.max_message_size)
        if size == 0:
            return b""

        return self._read_exactly(sock, size)

    def _read_exactly(self, sock: socket.socket, size: int) -> bytes:
        buffer = bytearray()
        while len(buffer) < size:
            try:
                chunk = sock.recv(size - len(buffer))
            except TimeoutError as error:
                raise FQTimeoutError(f"deadline exceeded while reading: {error}") from error
            except OSError as error:
                raise FQConnectionError(f"connection lost while reading: {error}") from error

            if not chunk:
                raise FQConnectionError("connection closed by server")

            buffer.extend(chunk)

        return bytes(buffer)

    def _apply_timeout(self, sock: socket.socket, deadline: Deadline) -> None:
        deadline.check()
        try:
            sock.settimeout(deadline.remaining())
        except OSError as error:
            raise FQConnectionError(f"connection is unusable: {error}") from error

    def _require_socket(self) -> socket.socket:
        if self._sock is None:
            raise FQConnectionError("connection is closed")

        return self._sock
