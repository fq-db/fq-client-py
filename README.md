# fq-client-py

Python client for the [fq database](https://github.com/fq-db/fq).

Sync and asyncio clients over one protocol core. No runtime dependencies, Python 3.11+.

## Install

```shell
pip install fq-client
```

## Usage

```python
from fq import CappingKey, Client, LimitKey

with Client("127.0.0.1:1945", pool_size=8) as client:
    value = client.incr(CappingKey("user_42", capping=60))
    result = client.rlimit_sliding_window(LimitKey("ip_1.2.3.4", window=60), limit=100)
    if not result.allowed:
        raise RuntimeError(f"rate limited, retry after {result.reset_after}s")
```

The asyncio client mirrors the sync one:

```python
from fq import AsyncClient, CappingKey

async with AsyncClient("127.0.0.1:1945", pool_size=8) as client:
    value = await client.incr(CappingKey("user_42", capping=60))
```

## Streams

Streams are iterators, and reconnection happens inside them, so a dropped connection
does not end the loop:

```python
for event in client.pstream("user_"):
    print(event.key, event.current)

async with aclosing(client.qstream()) as stream:
    async for event in stream:
        print(event.event, event.name)
```

## Timeouts and reconnection

Every call has a deadline - `timeout` from the constructor, overridable per call. Within
that deadline the client reconnects as many times as it needs to, with exponential
backoff and full jitter, so a restarted server does not require restarting the
application:

```python
from fq import Client, ReconnectPolicy

client = Client(
    "127.0.0.1:1945",
    timeout=5.0,
    reconnect=ReconnectPolicy(initial=0.05, max_delay=2.0),
)

client.get(key, timeout=0.5)  # tighter deadline for one call
client.watch(key, timeout=30)  # WATCH blocks until the value changes
```

Streams have no deadline, so they reconnect indefinitely.

A command whose request already reached the socket is retried only when it is read-only.
`INCR`, `RLIMIT` and `QUOTA ACQ` raise `FQConnectionError` instead, because a blind retry
could apply them twice.

## Sharding

```python
import os

from fq import CappingKey, ShardedClient

with ShardedClient(
    ["fq-a.internal:1945", "fq-b.internal:1945"],
    token=os.environ["FQ_TOKEN"],
) as client:
    client.incr(CappingKey("user_42", capping=60))
```

Counters and rate limits are routed by key, quotas by quota name. `mdelete` is split
across shards and returns results in the input order. `flushdb`, `truncate`, `inspect`
and streams fan out to every shard. `scan` and `pscan` walk shards with an opaque
composite cursor. The default sharding function is FNV-1a; pass your own with
`sharding_func=`.

## Authentication

```python
client = Client("127.0.0.1:1945", token=os.environ["FQ_TOKEN"])
```

The token travels inline with the mandatory `HELLO` handshake and is re-sent after every
reconnect, so pooled connections stay authenticated. A rejected token raises
`AuthenticationFailedError` at construction. Commands the token's role does not cover
raise `AuthError` with `code == ErrorCode.PERMISSION_DENIED`.

## TLS

```python
from fq import Client, TLSConfig

client = Client(
    "fq.internal:1945",
    tls=TLSConfig(ca_file="/etc/fq/ca.crt", server_name="fq.internal"),
)
```

For mutual TLS add the client keypair, which the server requires whenever it sets
`client_ca_file`:

```python
TLSConfig(
    ca_file="/etc/fq/ca.crt",
    cert_file="/etc/fq/client.crt",
    key_file="/etc/fq/client.key",
    server_name="fq.internal",
)
```

| Field | Meaning |
|---|---|
| `ca_file` | trust anchor for the server certificate; system roots are used when empty |
| `cert_file` / `key_file` | client keypair for mutual TLS; must be set together |
| `server_name` | expected name in the server certificate; defaults to the dialed host |
| `skip_verify` | disables server certificate verification; local testing only |
| `min_version` | `1.2` (default) or `1.3` |

When certificates come from somewhere other than the filesystem, build the
`ssl.SSLContext` yourself and pass it with `ssl_context=`.

## Errors

Error classes follow the protocol's code ranges, and the exact code is always available
in `.code`:

| Class | Codes |
|---|---|
| `RequestError` | 1xxx |
| `ArgumentError` | 2xxx |
| `AuthError` (`AuthenticationFailedError` for 3002) | 3xxx |
| `QuotaError` | 4xxx |
| `InstanceStateError` | 5xxx |
| `InternalError` | 9xxx |

Transport failures raise `FQConnectionError` (`FQTimeoutError` for an expired deadline),
and a malformed response raises `CorruptedResponseError`.

## Tests

```shell
make test
make test-integration
```

The integration suite runs the client against a real fq server in Docker
(`ghcr.io/fq-db/fq:latest`) and is skipped when Docker is unavailable.
