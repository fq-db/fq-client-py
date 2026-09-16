"""Sharded synchronous client."""

from __future__ import annotations

import queue
import threading
from collections.abc import Callable, Iterator, Sequence
from typing import Any, TypeVar

from fq._sharding import (
    ShardingFunc,
    fnv1a_shard,
    group_by_shard,
    make_shard_cursor,
    parse_shard_cursor,
    resolve_shard,
)
from fq.errors import ShardIndexError
from fq.inspect_types import InspectReport
from fq.sync.client import DEFAULT, STREAM_POLL_INTERVAL, Client, Timeout
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


class ShardedClient:
    """One logical database spread across several fq instances.

    Counters and limiters are routed by key, quotas by quota name. ``mdelete``
    is split across shards and reassembled in the input order; maintenance
    commands, ``INSPECT`` and streams fan out.
    """

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
        self._shards: list[Client] = []

        try:
            for address in addresses:
                self._shards.append(Client(address, **client_options))
        except BaseException:
            self.close()

            raise

    def __enter__(self) -> ShardedClient:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    def close(self) -> None:
        for shard in self._shards:
            shard.close()

    @property
    def shard_count(self) -> int:
        return len(self._shards)

    def shard(self, index: int) -> Client:
        """Direct access to a single shard's client."""
        if index < 0 or index >= len(self._shards):
            raise ShardIndexError(f"shard {index} is out of range for {len(self._shards)} shards")

        return self._shards[index]

    def incr(self, key: CappingKey, *, timeout: Timeout = DEFAULT) -> int:
        return self._by_key(key.key).incr(key, timeout=timeout)

    def incrby(self, key: CappingKey, value: int, *, timeout: Timeout = DEFAULT) -> int:
        return self._by_key(key.key).incrby(key, value, timeout=timeout)

    def get(self, key: CappingKey, *, timeout: Timeout = DEFAULT) -> int:
        return self._by_key(key.key).get(key, timeout=timeout)

    def watch(self, key: CappingKey, *, timeout: Timeout = DEFAULT) -> int:
        return self._by_key(key.key).watch(key, timeout=timeout)

    def delete(self, key: CappingKey, *, timeout: Timeout = DEFAULT) -> bool:
        return self._by_key(key.key).delete(key, timeout=timeout)

    def mdelete(
        self,
        keys: Sequence[CappingKey],
        *,
        timeout: Timeout = DEFAULT,
    ) -> tuple[bool, ...]:
        results: list[bool] = [False] * len(keys)

        for index, indexed_keys in group_by_shard(keys, self._index_of).items():
            shard_results = self._shards[index].mdelete(
                [key for _, key in indexed_keys],
                timeout=timeout,
            )
            if len(shard_results) != len(indexed_keys):
                raise ValueError("shard returned a different number of results than requested")

            for (position, _), result in zip(indexed_keys, shard_results, strict=True):
                results[position] = result

        return tuple(results)

    def rlimit_fixed_window(
        self,
        key: LimitKey,
        limit: int,
        *,
        timeout: Timeout = DEFAULT,
    ) -> RateLimitResult:
        return self._by_key(key.key).rlimit_fixed_window(key, limit, timeout=timeout)

    def rlimit_sliding_window(
        self,
        key: LimitKey,
        limit: int,
        *,
        timeout: Timeout = DEFAULT,
    ) -> RateLimitResult:
        return self._by_key(key.key).rlimit_sliding_window(key, limit, timeout=timeout)

    def rlimit_token_bucket(
        self,
        key: LimitKey,
        capacity: int,
        refill_amount: int,
        *,
        timeout: Timeout = DEFAULT,
    ) -> RateLimitResult:
        return self._by_key(key.key).rlimit_token_bucket(
            key,
            capacity,
            refill_amount,
            timeout=timeout,
        )

    def quota_set(self, name: str, limit: int, *, timeout: Timeout = DEFAULT) -> bool:
        return self._by_key(name).quota_set(name, limit, timeout=timeout)

    def quota_set_n(
        self,
        name: str,
        limit: int,
        clients: int,
        *,
        timeout: Timeout = DEFAULT,
    ) -> bool:
        return self._by_key(name).quota_set_n(name, limit, clients, timeout=timeout)

    def quota_acquire(
        self,
        name: str,
        amount: int,
        client_id: str,
        ttl: int | None = None,
        *,
        timeout: Timeout = DEFAULT,
    ) -> QuotaAcquireResult:
        return self._by_key(name).quota_acquire(name, amount, client_id, ttl, timeout=timeout)

    def quota_acquire_n(
        self,
        name: str,
        client_id: str,
        ttl: int | None = None,
        *,
        timeout: Timeout = DEFAULT,
    ) -> QuotaAcquireResult:
        return self._by_key(name).quota_acquire_n(name, client_id, ttl, timeout=timeout)

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
        return self._by_key(name).quota_acquire_lease(
            name,
            limit,
            amount,
            client_id,
            ttl,
            timeout=timeout,
        )

    def quota_release(self, name: str, client_id: str, *, timeout: Timeout = DEFAULT) -> bool:
        return self._by_key(name).quota_release(name, client_id, timeout=timeout)

    def quota_delete(self, name: str, *, timeout: Timeout = DEFAULT) -> bool:
        return self._by_key(name).quota_delete(name, timeout=timeout)

    def quota_info(self, name: str, *, timeout: Timeout = DEFAULT) -> QuotaInfo:
        return self._by_key(name).quota_info(name, timeout=timeout)

    def scan(
        self,
        count: int,
        cursor: str = SCAN_CURSOR_INITIAL,
        *,
        timeout: Timeout = DEFAULT,
    ) -> ScanResult:
        return self._walk(
            cursor,
            count,
            lambda shard, shard_cursor, remaining: shard.scan(
                remaining,
                shard_cursor,
                timeout=timeout,
            ),
        )

    def pscan(
        self,
        prefix: str,
        count: int,
        cursor: str = SCAN_CURSOR_INITIAL,
        *,
        timeout: Timeout = DEFAULT,
    ) -> ScanResult:
        return self._walk(
            cursor,
            count,
            lambda shard, shard_cursor, remaining: shard.pscan(
                prefix,
                remaining,
                shard_cursor,
                timeout=timeout,
            ),
        )

    def flushdb(self, *, timeout: Timeout = DEFAULT) -> bool:
        return all([shard.flushdb(timeout=timeout) for shard in self._shards])

    def truncate(self, *, timeout: Timeout = DEFAULT) -> bool:
        return all([shard.truncate(timeout=timeout) for shard in self._shards])

    def inspect(
        self,
        section: InspectSection = InspectSection.SUMMARY,
        *,
        timeout: Timeout = DEFAULT,
    ) -> tuple[InspectReport, ...]:
        return tuple(shard.inspect(section, timeout=timeout) for shard in self._shards)

    def stream(self) -> Iterator[LimitEvent]:
        return self._fan_out(lambda shard, stop: shard.stream_polled(stop))

    def pstream(self, prefix: str) -> Iterator[LimitEvent]:
        return self._fan_out(lambda shard, stop: shard.pstream_polled(prefix, stop))

    def qstream(self) -> Iterator[QuotaEvent]:
        return self._fan_out(lambda shard, stop: shard.qstream_polled(stop))

    def qpstream(self, prefix: str) -> Iterator[QuotaEvent]:
        return self._fan_out(lambda shard, stop: shard.qpstream_polled(prefix, stop))

    def _index_of(self, key: str) -> int:
        return resolve_shard(key, len(self._shards), self._sharding_func)

    def _by_key(self, key: str) -> Client:
        return self._shards[self._index_of(key)]

    def _walk(
        self,
        cursor: str,
        count: int,
        scan_shard: Callable[[Client, str, int], ScanResult],
    ) -> ScanResult:
        index, shard_cursor = parse_shard_cursor(cursor, len(self._shards))
        keys: list[ScanKey] = []

        while index < len(self._shards) and len(keys) < count:
            page = scan_shard(self._shards[index], shard_cursor, count - len(keys))
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

    def _fan_out(
        self,
        open_stream: Callable[[Client, Callable[[], bool]], Iterator[T]],
    ) -> Iterator[T]:
        events: queue.Queue[tuple[T | None, BaseException | None]] = queue.Queue()
        stop = threading.Event()

        def worker(shard: Client) -> None:
            try:
                for event in open_stream(shard, stop.is_set):
                    events.put((event, None))
                    if stop.is_set():
                        return
            except BaseException as error:
                events.put((None, error))

        threads = [
            threading.Thread(target=worker, args=(shard,), daemon=True) for shard in self._shards
        ]
        for thread in threads:
            thread.start()

        try:
            while True:
                event, error = events.get()
                if error is not None:
                    raise error

                assert event is not None
                yield event
        finally:
            stop.set()
            for thread in threads:
                thread.join(timeout=4 * STREAM_POLL_INTERVAL)
