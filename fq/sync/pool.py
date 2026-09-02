"""Pool of synchronous connections."""

from __future__ import annotations

import queue
from collections.abc import Callable, Iterator
from contextlib import contextmanager

from fq._reconnect import Deadline
from fq.errors import FQTimeoutError, PoolClosedError
from fq.sync.connection import Connection


class ConnectionPool:
    """A bounded pool handing out connections LIFO.

    LIFO keeps hot connections in use while cold ones sit idle and are the first
    to be dropped by the server's ``idle_timeout``.
    """

    def __init__(self, size: int, factory: Callable[[], Connection]) -> None:
        if size < 1:
            raise ValueError("pool_size must be at least 1")

        self.size = size
        self._factory = factory
        self._slots: queue.LifoQueue[Connection | None] = queue.LifoQueue(maxsize=size)
        self._closed = False

        for _ in range(size):
            self._slots.put(None)

    def prefill(self) -> None:
        """Open every connection upfront so failures surface at startup."""
        opened: list[Connection] = []
        try:
            for _ in range(self.size):
                self._slots.get_nowait()
                opened.append(self._factory())
        except BaseException:
            for connection in opened:
                connection.close()
            for _ in range(len(opened)):
                self._slots.put(None)

            raise

        for connection in opened:
            self._slots.put(connection)

    @contextmanager
    def acquire(self, deadline: Deadline) -> Iterator[Connection]:
        """Hold a connection for the duration of the ``with`` block."""
        if self._closed:
            raise PoolClosedError("client is closed")

        slot = self._take(deadline)
        try:
            connection = slot if slot is not None else self._factory()
        except BaseException:
            self._slots.put(None)

            raise

        try:
            yield connection
        finally:
            self._release(connection)

    def close(self) -> None:
        """Close the pool and every connection in it."""
        if self._closed:
            return

        self._closed = True
        while True:
            try:
                slot = self._slots.get_nowait()
            except queue.Empty:
                return

            if slot is not None:
                slot.close()

    def _take(self, deadline: Deadline) -> Connection | None:
        remaining = deadline.remaining()
        try:
            if remaining is None:
                return self._slots.get()

            return self._slots.get(timeout=max(remaining, 0.0))
        except queue.Empty as error:
            raise FQTimeoutError("no free connection in the pool") from error

    def _release(self, connection: Connection) -> None:
        if self._closed:
            connection.close()

            return

        self._slots.put(None if connection.closed else connection)
