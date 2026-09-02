"""Asynchronous fq client."""

from __future__ import annotations

import asyncio
import ssl
from collections.abc import AsyncGenerator, Callable, Sequence
from typing import Any, Self, TypeVar

from fq import _commands as commands
from fq import _responses as responses
from fq._commands import Command
from fq._reconnect import Deadline, ReconnectPolicy
from fq.aio.connection import AsyncConnection
from fq.aio.pool import AsyncConnectionPool
from fq.errors import FQConnectionError, FQTimeoutError
from fq.inspect_types import InspectReport, parse_inspect
from fq.sync.client import DEFAULT, Timeout, _Default
from fq.types import (
    SCAN_CURSOR_INITIAL,
    CappingKey,
    InspectSection,
    LimitEvent,
    LimitKey,
    QuotaAcquireResult,
    QuotaEvent,
    QuotaInfo,
    RateLimitResult,
    ScanResult,
    TLSConfig,
)

T = TypeVar("T")


class AsyncClient:
    """Asyncio client of a single fq instance.

    The constructor performs no I/O: connections open in ``await connect()`` or
    on entering ``async with``.
    """

    def __init__(
        self,
        address: str,
        *,
        pool_size: int = 8,
        timeout: float | None = 5.0,
        connect_timeout: float = 5.0,
        token: str | None = None,
        tls: TLSConfig | None = None,
        ssl_context: ssl.SSLContext | None = None,
        server_hostname: str | None = None,
        reconnect: ReconnectPolicy | None = None,
    ) -> None:
        if tls is not None and ssl_context is not None:
            raise ValueError("pass either tls or ssl_context, not both")

        context = tls.build_context() if tls is not None else ssl_context
        hostname = server_hostname or (tls.server_name if tls is not None else None) or None

        self._timeout = timeout
        self._connect_timeout = connect_timeout
        self._reconnect = reconnect if reconnect is not None else ReconnectPolicy()
        self.server_role = ""
        self.auth_required = False

        async def factory() -> AsyncConnection:
            connection = AsyncConnection(
                address,
                token=token,
                ssl_context=context,
                server_hostname=hostname,
                connect_timeout=connect_timeout,
            )
            await connection.connect()

            return connection

        self._pool = AsyncConnectionPool(pool_size, factory)
        self._opened = False

    @classmethod
    async def connect(cls, address: str, **options: Any) -> Self:
        """Create a client and open every connection."""
        client = cls(address, **options)
        await client.open()

        return client

    async def open(self) -> None:
        """Open the pool and record the role the server granted."""
        if self._opened:
            return

        await self._pool.prefill()
        async with self._pool.acquire(Deadline(self._connect_timeout)) as connection:
            self.server_role = connection.role
            self.auth_required = connection.auth_required

        self._opened = True

    async def __aenter__(self) -> Self:
        await self.open()

        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        await self._pool.aclose()

    async def incr(self, key: CappingKey, *, timeout: Timeout = DEFAULT) -> int:
        return await self._execute(commands.incr(key), responses.parse_value, timeout)

    async def get(self, key: CappingKey, *, timeout: Timeout = DEFAULT) -> int:
        return await self._execute(commands.get(key), responses.parse_value, timeout)

    async def watch(self, key: CappingKey, *, timeout: Timeout = DEFAULT) -> int:
        return await self._execute(commands.watch(key), responses.parse_value, timeout)

    async def delete(self, key: CappingKey, *, timeout: Timeout = DEFAULT) -> bool:
        return await self._execute(commands.delete(key), responses.parse_bool, timeout)

    async def mdelete(
        self,
        keys: Sequence[CappingKey],
        *,
        timeout: Timeout = DEFAULT,
    ) -> tuple[bool, ...]:
        return await self._execute(commands.mdelete(keys), responses.parse_bools, timeout)

    async def rlimit_fixed_window(
        self,
        key: LimitKey,
        limit: int,
        *,
        timeout: Timeout = DEFAULT,
    ) -> RateLimitResult:
        command = commands.rlimit_fixed_window(key, limit)

        return await self._execute(command, responses.parse_rate_limit, timeout)

    async def rlimit_sliding_window(
        self,
        key: LimitKey,
        limit: int,
        *,
        timeout: Timeout = DEFAULT,
    ) -> RateLimitResult:
        command = commands.rlimit_sliding_window(key, limit)

        return await self._execute(command, responses.parse_rate_limit, timeout)

    async def rlimit_token_bucket(
        self,
        key: LimitKey,
        capacity: int,
        refill_amount: int,
        *,
        timeout: Timeout = DEFAULT,
    ) -> RateLimitResult:
        command = commands.rlimit_token_bucket(key, capacity, refill_amount)

        return await self._execute(command, responses.parse_rate_limit, timeout)

    async def quota_set(self, name: str, limit: int, *, timeout: Timeout = DEFAULT) -> bool:
        command = commands.quota_set(name, limit)

        return await self._execute(command, responses.parse_bool, timeout)

    async def quota_set_n(
        self,
        name: str,
        limit: int,
        clients: int,
        *,
        timeout: Timeout = DEFAULT,
    ) -> bool:
        command = commands.quota_set_n(name, limit, clients)

        return await self._execute(command, responses.parse_bool, timeout)

    async def quota_acquire(
        self,
        name: str,
        amount: int,
        client_id: str,
        ttl: int | None = None,
        *,
        timeout: Timeout = DEFAULT,
    ) -> QuotaAcquireResult:
        command = commands.quota_acquire(name, amount, client_id, ttl)

        return await self._execute(command, responses.parse_quota_acquire, timeout)

    async def quota_acquire_n(
        self,
        name: str,
        client_id: str,
        ttl: int | None = None,
        *,
        timeout: Timeout = DEFAULT,
    ) -> QuotaAcquireResult:
        command = commands.quota_acquire_n(name, client_id, ttl)

        return await self._execute(command, responses.parse_quota_acquire, timeout)

    async def quota_acquire_lease(
        self,
        name: str,
        limit: int,
        amount: int,
        client_id: str,
        ttl: int | None = None,
        *,
        timeout: Timeout = DEFAULT,
    ) -> QuotaAcquireResult:
        command = commands.quota_acquire_lease(name, limit, amount, client_id, ttl)

        return await self._execute(command, responses.parse_quota_acquire, timeout)

    async def quota_release(
        self,
        name: str,
        client_id: str,
        *,
        timeout: Timeout = DEFAULT,
    ) -> bool:
        command = commands.quota_release(name, client_id)

        return await self._execute(command, responses.parse_bool, timeout)

    async def quota_delete(self, name: str, *, timeout: Timeout = DEFAULT) -> bool:
        command = commands.quota_delete(name)

        return await self._execute(command, responses.parse_bool, timeout)

    async def quota_info(self, name: str, *, timeout: Timeout = DEFAULT) -> QuotaInfo:
        command = commands.quota_info(name)

        return await self._execute(command, responses.parse_quota_info, timeout)

    async def scan(
        self,
        count: int,
        cursor: str = SCAN_CURSOR_INITIAL,
        *,
        timeout: Timeout = DEFAULT,
    ) -> ScanResult:
        command = commands.scan(count, cursor)

        return await self._execute(command, responses.parse_scan, timeout)

    async def pscan(
        self,
        prefix: str,
        count: int,
        cursor: str = SCAN_CURSOR_INITIAL,
        *,
        timeout: Timeout = DEFAULT,
    ) -> ScanResult:
        command = commands.pscan(prefix, count, cursor)

        return await self._execute(command, responses.parse_scan, timeout)

    async def flushdb(self, *, timeout: Timeout = DEFAULT) -> bool:
        return await self._execute(commands.flushdb(), responses.parse_bool, timeout)

    async def truncate(self, *, timeout: Timeout = DEFAULT) -> bool:
        return await self._execute(commands.truncate(), responses.parse_bool, timeout)

    async def inspect(
        self,
        section: InspectSection = InspectSection.SUMMARY,
        *,
        timeout: Timeout = DEFAULT,
    ) -> InspectReport:
        deadline = self._deadline(timeout)
        async with self._pool.acquire(deadline) as connection:
            payload = await self._with_retry(
                connection,
                commands.inspect(section),
                deadline,
                chunked=True,
            )

        return parse_inspect(payload)

    def stream(self) -> AsyncGenerator[LimitEvent, None]:
        return self._iter_stream(commands.stream(), responses.parse_limit_event)

    def pstream(self, prefix: str) -> AsyncGenerator[LimitEvent, None]:
        return self._iter_stream(commands.pstream(prefix), responses.parse_limit_event)

    def qstream(self) -> AsyncGenerator[QuotaEvent, None]:
        return self._iter_stream(commands.qstream(), responses.parse_quota_event)

    def qpstream(self, prefix: str) -> AsyncGenerator[QuotaEvent, None]:
        return self._iter_stream(commands.qpstream(prefix), responses.parse_quota_event)

    def _deadline(self, timeout: Timeout) -> Deadline:
        value = self._timeout if isinstance(timeout, _Default) else timeout

        return Deadline(value)

    async def _execute(
        self,
        command: Command,
        parse: Callable[[bytes], T],
        timeout: Timeout,
    ) -> T:
        deadline = self._deadline(timeout)
        async with self._pool.acquire(deadline) as connection:
            payload = await self._with_retry(connection, command, deadline, chunked=False)

        return parse(payload)

    async def _with_retry(
        self,
        connection: AsyncConnection,
        command: Command,
        deadline: Deadline,
        *,
        chunked: bool,
    ) -> bytes:
        attempt = 0
        while True:
            try:
                if chunked:
                    return await connection.send_chunked(command, deadline)

                return await connection.send(command, deadline)
            except FQTimeoutError:
                raise
            except FQConnectionError:
                if connection.request_in_flight and not command.read_only:
                    raise

                attempt += 1
                if self._reconnect.exhausted(attempt):
                    raise

                await self._backoff(attempt, deadline)

                try:
                    await connection.reconnect()
                except FQTimeoutError:
                    raise
                except FQConnectionError:
                    continue

    async def _backoff(self, attempt: int, deadline: Deadline) -> None:
        deadline.check()
        delay = deadline.clamp(self._reconnect.delay(attempt))
        if delay > 0:
            await asyncio.sleep(delay)
        deadline.check()

    async def _iter_stream(
        self,
        command: Command,
        parse: Callable[[bytes], T],
    ) -> AsyncGenerator[T, None]:
        async with self._pool.acquire(Deadline(self._connect_timeout)) as connection:
            attempt = 0
            while True:
                try:
                    async for frame in connection.open_stream(command):
                        if frame is None:
                            continue

                        yield parse(frame)
                        attempt = 0

                    return
                except FQConnectionError:
                    attempt += 1
                    if self._reconnect.exhausted(attempt):
                        raise

                    await asyncio.sleep(self._reconnect.delay(attempt))

                    try:
                        await connection.reconnect()
                    except FQConnectionError:
                        continue
