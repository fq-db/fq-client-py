"""Synchronous fq client."""

from __future__ import annotations

import ssl
import time
from collections.abc import Callable, Iterator, Sequence
from typing import TypeVar

from fq import _commands as commands
from fq import _responses as responses
from fq._commands import Command
from fq._reconnect import Deadline, ReconnectPolicy
from fq.errors import FQConnectionError, FQTimeoutError
from fq.inspect_types import InspectReport, parse_inspect
from fq.sync.connection import Connection
from fq.sync.pool import ConnectionPool
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

STREAM_POLL_INTERVAL = 0.25


class _Default:
    """Marker meaning "take the value from the client settings"."""

    __slots__ = ()


DEFAULT = _Default()
Timeout = float | None | _Default

Sender = Callable[[Connection, Command, Deadline], bytes]


class Client:
    """Client of a single fq instance."""

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

        def factory() -> Connection:
            connection = Connection(
                address,
                token=token,
                ssl_context=context,
                server_hostname=hostname,
                connect_timeout=connect_timeout,
            )
            connection.connect()

            return connection

        self._pool = ConnectionPool(pool_size, factory)
        self._pool.prefill()

        with self._pool.acquire(Deadline(connect_timeout)) as connection:
            self.server_role = connection.role
            self.auth_required = connection.auth_required

    def __enter__(self) -> Client:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    def close(self) -> None:
        """Close every connection."""
        self._pool.close()

    def incr(self, key: CappingKey, *, timeout: Timeout = DEFAULT) -> int:
        return self._execute(commands.incr(key), responses.parse_value, timeout)

    def get(self, key: CappingKey, *, timeout: Timeout = DEFAULT) -> int:
        return self._execute(commands.get(key), responses.parse_value, timeout)

    def watch(self, key: CappingKey, *, timeout: Timeout = DEFAULT) -> int:
        return self._execute(commands.watch(key), responses.parse_value, timeout)

    def delete(self, key: CappingKey, *, timeout: Timeout = DEFAULT) -> bool:
        return self._execute(commands.delete(key), responses.parse_bool, timeout)

    def mdelete(
        self,
        keys: Sequence[CappingKey],
        *,
        timeout: Timeout = DEFAULT,
    ) -> tuple[bool, ...]:
        return self._execute(commands.mdelete(keys), responses.parse_bools, timeout)

    def rlimit_fixed_window(
        self,
        key: LimitKey,
        limit: int,
        *,
        timeout: Timeout = DEFAULT,
    ) -> RateLimitResult:
        command = commands.rlimit_fixed_window(key, limit)

        return self._execute(command, responses.parse_rate_limit, timeout)

    def rlimit_sliding_window(
        self,
        key: LimitKey,
        limit: int,
        *,
        timeout: Timeout = DEFAULT,
    ) -> RateLimitResult:
        command = commands.rlimit_sliding_window(key, limit)

        return self._execute(command, responses.parse_rate_limit, timeout)

    def rlimit_token_bucket(
        self,
        key: LimitKey,
        capacity: int,
        refill_amount: int,
        *,
        timeout: Timeout = DEFAULT,
    ) -> RateLimitResult:
        command = commands.rlimit_token_bucket(key, capacity, refill_amount)

        return self._execute(command, responses.parse_rate_limit, timeout)

    def quota_set(self, name: str, limit: int, *, timeout: Timeout = DEFAULT) -> bool:
        return self._execute(commands.quota_set(name, limit), responses.parse_bool, timeout)

    def quota_set_n(
        self,
        name: str,
        limit: int,
        clients: int,
        *,
        timeout: Timeout = DEFAULT,
    ) -> bool:
        command = commands.quota_set_n(name, limit, clients)

        return self._execute(command, responses.parse_bool, timeout)

    def quota_acquire(
        self,
        name: str,
        amount: int,
        client_id: str,
        ttl: int | None = None,
        *,
        timeout: Timeout = DEFAULT,
    ) -> QuotaAcquireResult:
        command = commands.quota_acquire(name, amount, client_id, ttl)

        return self._execute(command, responses.parse_quota_acquire, timeout)

    def quota_acquire_n(
        self,
        name: str,
        client_id: str,
        ttl: int | None = None,
        *,
        timeout: Timeout = DEFAULT,
    ) -> QuotaAcquireResult:
        command = commands.quota_acquire_n(name, client_id, ttl)

        return self._execute(command, responses.parse_quota_acquire, timeout)

    def quota_acquire_lease(
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

        return self._execute(command, responses.parse_quota_acquire, timeout)

    def quota_release(self, name: str, client_id: str, *, timeout: Timeout = DEFAULT) -> bool:
        command = commands.quota_release(name, client_id)

        return self._execute(command, responses.parse_bool, timeout)

    def quota_delete(self, name: str, *, timeout: Timeout = DEFAULT) -> bool:
        return self._execute(commands.quota_delete(name), responses.parse_bool, timeout)

    def quota_info(self, name: str, *, timeout: Timeout = DEFAULT) -> QuotaInfo:
        return self._execute(commands.quota_info(name), responses.parse_quota_info, timeout)

    def scan(
        self,
        count: int,
        cursor: str = SCAN_CURSOR_INITIAL,
        *,
        timeout: Timeout = DEFAULT,
    ) -> ScanResult:
        return self._execute(commands.scan(count, cursor), responses.parse_scan, timeout)

    def pscan(
        self,
        prefix: str,
        count: int,
        cursor: str = SCAN_CURSOR_INITIAL,
        *,
        timeout: Timeout = DEFAULT,
    ) -> ScanResult:
        command = commands.pscan(prefix, count, cursor)

        return self._execute(command, responses.parse_scan, timeout)

    def flushdb(self, *, timeout: Timeout = DEFAULT) -> bool:
        return self._execute(commands.flushdb(), responses.parse_bool, timeout)

    def truncate(self, *, timeout: Timeout = DEFAULT) -> bool:
        return self._execute(commands.truncate(), responses.parse_bool, timeout)

    def inspect(
        self,
        section: InspectSection = InspectSection.SUMMARY,
        *,
        timeout: Timeout = DEFAULT,
    ) -> InspectReport:
        deadline = self._deadline(timeout)
        with self._pool.acquire(deadline) as connection:
            payload = self._with_retry(
                connection,
                commands.inspect(section),
                deadline,
                Connection.send_chunked,
            )

        return parse_inspect(payload)

    def stream(self) -> Iterator[LimitEvent]:
        return self._stream(commands.stream(), responses.parse_limit_event)

    def pstream(self, prefix: str) -> Iterator[LimitEvent]:
        return self._stream(commands.pstream(prefix), responses.parse_limit_event)

    def qstream(self) -> Iterator[QuotaEvent]:
        return self._stream(commands.qstream(), responses.parse_quota_event)

    def qpstream(self, prefix: str) -> Iterator[QuotaEvent]:
        return self._stream(commands.qpstream(prefix), responses.parse_quota_event)

    def stream_polled(self, should_stop: Callable[[], bool]) -> Iterator[LimitEvent]:
        return self._stream(commands.stream(), responses.parse_limit_event, should_stop)

    def pstream_polled(
        self,
        prefix: str,
        should_stop: Callable[[], bool],
    ) -> Iterator[LimitEvent]:
        return self._stream(commands.pstream(prefix), responses.parse_limit_event, should_stop)

    def qstream_polled(self, should_stop: Callable[[], bool]) -> Iterator[QuotaEvent]:
        return self._stream(commands.qstream(), responses.parse_quota_event, should_stop)

    def qpstream_polled(
        self,
        prefix: str,
        should_stop: Callable[[], bool],
    ) -> Iterator[QuotaEvent]:
        return self._stream(commands.qpstream(prefix), responses.parse_quota_event, should_stop)

    def _deadline(self, timeout: Timeout) -> Deadline:
        value = self._timeout if isinstance(timeout, _Default) else timeout

        return Deadline(value)

    def _execute(
        self,
        command: Command,
        parse: Callable[[bytes], T],
        timeout: Timeout,
    ) -> T:
        deadline = self._deadline(timeout)
        with self._pool.acquire(deadline) as connection:
            payload = self._with_retry(connection, command, deadline, Connection.send)

        return parse(payload)

    def _with_retry(
        self,
        connection: Connection,
        command: Command,
        deadline: Deadline,
        send: Sender,
    ) -> bytes:
        attempt = 0
        while True:
            try:
                return send(connection, command, deadline)
            except FQTimeoutError:
                raise
            except FQConnectionError:
                if connection.request_in_flight and not command.read_only:
                    raise

                attempt += 1
                if self._reconnect.exhausted(attempt):
                    raise

                self._backoff(attempt, deadline)

                try:
                    connection.reconnect()
                except FQTimeoutError:
                    raise
                except FQConnectionError:
                    continue

    def _backoff(self, attempt: int, deadline: Deadline) -> None:
        deadline.check()
        delay = deadline.clamp(self._reconnect.delay(attempt))
        if delay > 0:
            time.sleep(delay)
        deadline.check()

    def _stream(
        self,
        command: Command,
        parse: Callable[[bytes], T],
        should_stop: Callable[[], bool] | None = None,
    ) -> Iterator[T]:
        poll_interval = None if should_stop is None else STREAM_POLL_INTERVAL
        stop = should_stop if should_stop is not None else _never

        with self._pool.acquire(Deadline(self._connect_timeout)) as connection:
            attempt = 0
            while True:
                if stop():
                    return

                try:
                    for frame in connection.open_stream(command, poll_interval):
                        if frame is None:
                            if stop():
                                return

                            continue

                        yield parse(frame)
                        attempt = 0

                    return
                except FQConnectionError:
                    attempt += 1
                    if self._reconnect.exhausted(attempt):
                        raise

                    time.sleep(self._reconnect.delay(attempt))

                    try:
                        connection.reconnect()
                    except FQConnectionError:
                        continue


def _never() -> bool:
    return False
