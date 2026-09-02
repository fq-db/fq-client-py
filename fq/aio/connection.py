"""Asynchronous transport on top of asyncio."""

from __future__ import annotations

import asyncio
import contextlib
import ssl
from collections.abc import AsyncIterator

from fq._chunked import ChunkAssembler
from fq._commands import Command, hello
from fq._framing import FRAME_HEADER_SIZE, check_request_size, decode_frame_size, encode_frame
from fq._reconnect import Deadline
from fq._responses import parse_hello
from fq.errors import CorruptedResponseError, FQConnectionError, FQTimeoutError
from fq.sync.connection import split_address
from fq.types import DEFAULT_MAX_MESSAGE_SIZE, PROTOCOL_VERSION


class AsyncConnection:
    """One asyncio connection to an fq server."""

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

        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None
        self.max_message_size = DEFAULT_MAX_MESSAGE_SIZE
        self.role = ""
        self.auth_required = False
        self.request_in_flight = False

    @property
    def closed(self) -> bool:
        return self._writer is None

    async def connect(self) -> None:
        """Open the connection and run the handshake."""
        host, port = split_address(self._address)
        try:
            async with asyncio.timeout(self._connect_timeout):
                reader, writer = await asyncio.open_connection(
                    host,
                    port,
                    ssl=self._ssl_context,
                    server_hostname=(self._server_hostname or host)
                    if self._ssl_context is not None
                    else None,
                )
        except TimeoutError as error:
            raise FQTimeoutError(f"connect to {self._address} timed out") from error
        except OSError as error:
            raise FQConnectionError(f"cannot connect to {self._address}: {error}") from error

        self._reader = reader
        self._writer = writer
        self.max_message_size = DEFAULT_MAX_MESSAGE_SIZE
        self.request_in_flight = False

        try:
            await self._handshake()
        except BaseException:
            await self.close()

            raise

    async def reconnect(self) -> None:
        await self.close()
        await self.connect()

    async def close(self) -> None:
        writer, self._writer, self._reader = self._writer, None, None
        if writer is None:
            return

        writer.close()
        with contextlib.suppress(OSError, asyncio.CancelledError):
            await writer.wait_closed()

    async def send(self, command: Command, deadline: Deadline) -> bytes:
        """Send a command and read one response frame."""
        await self._write_request(command, deadline)
        response = await self._read_frame(deadline)
        self.request_in_flight = False

        return response

    async def send_chunked(self, command: Command, deadline: Deadline) -> bytes:
        """Send a command and assemble a response spanning several frames."""
        await self._write_request(command, deadline)

        assembler = ChunkAssembler()
        while True:
            frame = await self._read_frame(deadline)
            response = assembler.feed(frame)
            if response is not None:
                self.request_in_flight = False

                return response

    async def open_stream(
        self,
        command: Command,
        poll_interval: float | None = None,
    ) -> AsyncIterator[bytes | None]:
        """Send a command and yield incoming frames forever.

        With ``poll_interval`` set, yields ``None`` whenever no frame arrived
        within that interval.
        """
        await self._write_request(command, Deadline(None))

        while True:
            if poll_interval is None:
                yield await self._read_frame(Deadline(None))

                continue

            try:
                yield await self._read_frame(Deadline(poll_interval))
            except FQTimeoutError:
                yield None

    async def _handshake(self) -> None:
        payload = await self.send(hello(self._token), Deadline(self._connect_timeout))
        info = parse_hello(payload)
        if info.version != PROTOCOL_VERSION:
            raise CorruptedResponseError(f"server negotiated version {info.version}")

        self.max_message_size = info.max_message_size
        self.auth_required = info.auth_required
        self.role = info.role

    async def _write_request(self, command: Command, deadline: Deadline) -> None:
        check_request_size(command.payload, self.max_message_size)
        writer = self._require_writer()
        deadline.check()

        try:
            async with asyncio.timeout(deadline.remaining()):
                writer.write(encode_frame(command.payload))
                await writer.drain()
        except TimeoutError as error:
            self.request_in_flight = True

            raise FQTimeoutError("deadline exceeded while sending") from error
        except OSError as error:
            raise FQConnectionError(f"connection lost while sending: {error}") from error

        self.request_in_flight = True

    async def _read_frame(self, deadline: Deadline) -> bytes:
        reader = self._require_reader()
        deadline.check()

        try:
            async with asyncio.timeout(deadline.remaining()):
                header = await reader.readexactly(FRAME_HEADER_SIZE)
                size = decode_frame_size(header, self.max_message_size)
                if size == 0:
                    return b""

                return await reader.readexactly(size)
        except TimeoutError as error:
            raise FQTimeoutError("deadline exceeded while reading") from error
        except asyncio.IncompleteReadError as error:
            raise FQConnectionError("connection closed by server") from error
        except OSError as error:
            raise FQConnectionError(f"connection lost while reading: {error}") from error

    def _require_reader(self) -> asyncio.StreamReader:
        if self._reader is None:
            raise FQConnectionError("connection is closed")

        return self._reader

    def _require_writer(self) -> asyncio.StreamWriter:
        if self._writer is None:
            raise FQConnectionError("connection is closed")

        return self._writer
