"""Pool of asynchronous connections."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager

from fq._reconnect import Deadline
from fq.aio.connection import AsyncConnection
from fq.errors import FQTimeoutError, PoolClosedError


class AsyncConnectionPool:
    """A bounded pool of asyncio connections handed out LIFO."""

    def __init__(self, size: int, factory: Callable[[], Awaitable[AsyncConnection]]) -> None:
        if size < 1:
            raise ValueError("pool_size must be at least 1")

        self.size = size
        self._factory = factory
        self._slots: asyncio.LifoQueue[AsyncConnection | None] = asyncio.LifoQueue(maxsize=size)
        self._closed = False

        for _ in range(size):
            self._slots.put_nowait(None)

    async def prefill(self) -> None:
        """Open every connection upfront."""
        opened: list[AsyncConnection] = []
        try:
            for _ in range(self.size):
                self._slots.get_nowait()
                opened.append(await self._factory())
        except BaseException:
            for connection in opened:
                await connection.close()
            for _ in range(len(opened)):
                self._slots.put_nowait(None)

            raise

        for connection in opened:
            self._slots.put_nowait(connection)

    @asynccontextmanager
    async def acquire(self, deadline: Deadline) -> AsyncIterator[AsyncConnection]:
        """Hold a connection for the duration of the ``async with`` block."""
        if self._closed:
            raise PoolClosedError("client is closed")

        slot = await self._take(deadline)
        try:
            connection = slot if slot is not None else await self._factory()
        except BaseException:
            self._slots.put_nowait(None)

            raise

        try:
            yield connection
        finally:
            await self._release(connection)

    async def aclose(self) -> None:
        """Close the pool and every connection in it."""
        if self._closed:
            return

        self._closed = True
        while True:
            try:
                slot = self._slots.get_nowait()
            except asyncio.QueueEmpty:
                return

            if slot is not None:
                await slot.close()

    async def _take(self, deadline: Deadline) -> AsyncConnection | None:
        try:
            async with asyncio.timeout(deadline.remaining()):
                return await self._slots.get()
        except TimeoutError as error:
            raise FQTimeoutError("no free connection in the pool") from error

    async def _release(self, connection: AsyncConnection) -> None:
        if self._closed:
            await connection.close()

            return

        self._slots.put_nowait(None if connection.closed else connection)
