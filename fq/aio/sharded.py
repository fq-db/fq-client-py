"""Sharded asynchronous client."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator, AsyncIterator, Awaitable, Callable, Sequence
from typing import Any, Self, TypeVar

from fq._sharding import (
    ShardingFunc,
    fnv1a_shard,
    group_by_shard,
    make_shard_cursor,
    parse_shard_cursor,
    resolve_shard,
)
from fq.aio.client import AsyncClient
from fq.errors import ShardIndexError
from fq.inspect_types import InspectReport
from fq.sync.client import DEFAULT, Timeout
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
    ScanKey,
    ScanResult,
)

T = TypeVar("T")


class AsyncShardedClient:
    """One logical database spread across several fq instances."""

    def __init__(
        self,
        addresses: Sequence[str],
        *,
        sharding_func: ShardingFunc = fnv1a_shard,
        **client_options: Any,
    ) -> None:
        if not addresses:
            raise ValueError("at least one shard address is required")

        self._sharding_func = sharding_func
        self._shards = [AsyncClient(address, **client_options) for address in addresses]

    @classmethod
    async def connect(cls, addresses: Sequence[str], **options: Any) -> Self:
        client = cls(addresses, **options)
        await client.open()

        return client

    async def open(self) -> None:
        opened: list[AsyncClient] = []
        try:
            for shard in self._shards:
                await shard.open()
                opened.append(shard)
        except BaseException:
            for shard in opened:
                await shard.aclose()

            raise

    async def __aenter__(self) -> Self:
        await self.open()

        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        for shard in self._shards:
            await shard.aclose()

    @property
    def shard_count(self) -> int:
        return len(self._shards)

    def shard(self, index: int) -> AsyncClient:
        if index < 0 or index >= len(self._shards):
            raise ShardIndexError(f"shard {index} is out of range for {len(self._shards)} shards")

        return self._shards[index]

    async def incr(self, key: CappingKey, *, timeout: Timeout = DEFAULT) -> int:
        return await self._by_key(key.key).incr(key, timeout=timeout)

    async def incrby(self, key: CappingKey, value: int, *, timeout: Timeout = DEFAULT) -> int:
        return await self._by_key(key.key).incrby(key, value, timeout=timeout)

    async def get(self, key: CappingKey, *, timeout: Timeout = DEFAULT) -> int:
        return await self._by_key(key.key).get(key, timeout=timeout)

    async def watch(self, key: CappingKey, *, timeout: Timeout = DEFAULT) -> int:
        return await self._by_key(key.key).watch(key, timeout=timeout)

    async def delete(self, key: CappingKey, *, timeout: Timeout = DEFAULT) -> bool:
        return await self._by_key(key.key).delete(key, timeout=timeout)

    async def mdelete(
        self,
        keys: Sequence[CappingKey],
        *,
        timeout: Timeout = DEFAULT,
    ) -> tuple[bool, ...]:
        results: list[bool] = [False] * len(keys)

        for index, indexed_keys in group_by_shard(keys, self._index_of).items():
            shard_results = await self._shards[index].mdelete(
                [key for _, key in indexed_keys],
                timeout=timeout,
            )
            if len(shard_results) != len(indexed_keys):
                raise ValueError("shard returned a different number of results than requested")

            for (position, _), result in zip(indexed_keys, shard_results, strict=True):
                results[position] = result

        return tuple(results)

    async def rlimit_fixed_window(
        self,
        key: LimitKey,
        limit: int,
        *,
        timeout: Timeout = DEFAULT,
    ) -> RateLimitResult:
        return await self._by_key(key.key).rlimit_fixed_window(key, limit, timeout=timeout)

    async def rlimit_sliding_window(
        self,
        key: LimitKey,
        limit: int,
        *,
        timeout: Timeout = DEFAULT,
    ) -> RateLimitResult:
        return await self._by_key(key.key).rlimit_sliding_window(key, limit, timeout=timeout)

    async def rlimit_token_bucket(
        self,
        key: LimitKey,
        capacity: int,
        refill_amount: int,
        *,
        timeout: Timeout = DEFAULT,
    ) -> RateLimitResult:
        return await self._by_key(key.key).rlimit_token_bucket(
            key,
            capacity,
            refill_amount,
            timeout=timeout,
        )

    async def quota_set(self, name: str, limit: int, *, timeout: Timeout = DEFAULT) -> bool:
        return await self._by_key(name).quota_set(name, limit, timeout=timeout)

    async def quota_set_n(
        self,
        name: str,
        limit: int,
        clients: int,
        *,
        timeout: Timeout = DEFAULT,
    ) -> bool:
        return await self._by_key(name).quota_set_n(name, limit, clients, timeout=timeout)

    async def quota_acquire(
        self,
        name: str,
        amount: int,
        client_id: str,
        ttl: int | None = None,
        *,
        timeout: Timeout = DEFAULT,
    ) -> QuotaAcquireResult:
        return await self._by_key(name).quota_acquire(
            name,
            amount,
            client_id,
            ttl,
            timeout=timeout,
        )

    async def quota_acquire_n(
        self,
        name: str,
        client_id: str,
        ttl: int | None = None,
        *,
        timeout: Timeout = DEFAULT,
    ) -> QuotaAcquireResult:
        return await self._by_key(name).quota_acquire_n(name, client_id, ttl, timeout=timeout)

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
        return await self._by_key(name).quota_acquire_lease(
            name,
            limit,
            amount,
            client_id,
            ttl,
            timeout=timeout,
        )

    async def quota_release(
        self,
        name: str,
        client_id: str,
        *,
        timeout: Timeout = DEFAULT,
    ) -> bool:
        return await self._by_key(name).quota_release(name, client_id, timeout=timeout)

    async def quota_delete(self, name: str, *, timeout: Timeout = DEFAULT) -> bool:
        return await self._by_key(name).quota_delete(name, timeout=timeout)

    async def quota_info(self, name: str, *, timeout: Timeout = DEFAULT) -> QuotaInfo:
        return await self._by_key(name).quota_info(name, timeout=timeout)

    async def scan(
        self,
        count: int,
        cursor: str = SCAN_CURSOR_INITIAL,
        *,
        timeout: Timeout = DEFAULT,
    ) -> ScanResult:
        async def scan_shard(shard: AsyncClient, shard_cursor: str, remaining: int) -> ScanResult:
            return await shard.scan(remaining, shard_cursor, timeout=timeout)

        return await self._walk(cursor, count, scan_shard)

    async def pscan(
        self,
        prefix: str,
        count: int,
        cursor: str = SCAN_CURSOR_INITIAL,
        *,
        timeout: Timeout = DEFAULT,
    ) -> ScanResult:
        async def scan_shard(shard: AsyncClient, shard_cursor: str, remaining: int) -> ScanResult:
            return await shard.pscan(prefix, remaining, shard_cursor, timeout=timeout)

        return await self._walk(cursor, count, scan_shard)

    async def flushdb(self, *, timeout: Timeout = DEFAULT) -> bool:
        results = [await shard.flushdb(timeout=timeout) for shard in self._shards]

        return all(results)

    async def truncate(self, *, timeout: Timeout = DEFAULT) -> bool:
        results = [await shard.truncate(timeout=timeout) for shard in self._shards]

        return all(results)

    async def inspect(
        self,
        section: InspectSection = InspectSection.SUMMARY,
        *,
        timeout: Timeout = DEFAULT,
    ) -> tuple[InspectReport, ...]:
        return tuple([await shard.inspect(section, timeout=timeout) for shard in self._shards])

    def stream(self) -> AsyncGenerator[LimitEvent, None]:
        return self._fan_out(lambda shard: shard.stream())

    def pstream(self, prefix: str) -> AsyncGenerator[LimitEvent, None]:
        return self._fan_out(lambda shard: shard.pstream(prefix))

    def qstream(self) -> AsyncGenerator[QuotaEvent, None]:
        return self._fan_out(lambda shard: shard.qstream())

    def qpstream(self, prefix: str) -> AsyncGenerator[QuotaEvent, None]:
        return self._fan_out(lambda shard: shard.qpstream(prefix))

    def _index_of(self, key: str) -> int:
        return resolve_shard(key, len(self._shards), self._sharding_func)

    def _by_key(self, key: str) -> AsyncClient:
        return self._shards[self._index_of(key)]

    async def _walk(
        self,
        cursor: str,
        count: int,
        scan_shard: Callable[[AsyncClient, str, int], Awaitable[ScanResult]],
    ) -> ScanResult:
        index, shard_cursor = parse_shard_cursor(cursor, len(self._shards))
        keys: list[ScanKey] = []

        while index < len(self._shards) and len(keys) < count:
            page = await scan_shard(self._shards[index], shard_cursor, count - len(keys))
            keys.extend(page.keys)

            if page.cursor != SCAN_CURSOR_INITIAL:
                return ScanResult(
                    cursor=make_shard_cursor(index, page.cursor, len(self._shards)),
                    keys=tuple(keys),
                )

            index += 1
            shard_cursor = SCAN_CURSOR_INITIAL

        return ScanResult(
            cursor=make_shard_cursor(index, SCAN_CURSOR_INITIAL, len(self._shards)),
            keys=tuple(keys),
        )

    async def _fan_out(
        self,
        open_stream: Callable[[AsyncClient], AsyncIterator[T]],
    ) -> AsyncGenerator[T, None]:
        events: asyncio.Queue[tuple[T | None, BaseException | None]] = asyncio.Queue()

        async def worker(shard: AsyncClient) -> None:
            try:
                async for event in open_stream(shard):
                    await events.put((event, None))
            except asyncio.CancelledError:
                raise
            except BaseException as error:
                await events.put((None, error))

        tasks = [asyncio.create_task(worker(shard)) for shard in self._shards]
        try:
            while True:
                event, error = await events.get()
                if error is not None:
                    raise error

                assert event is not None
                yield event
        finally:
            for task in tasks:
                task.cancel()

            await asyncio.gather(*tasks, return_exceptions=True)
