"""Domain types of the fq client."""

from __future__ import annotations

import ssl
from dataclasses import dataclass
from enum import StrEnum

PROTOCOL_VERSION = 1
SCAN_CURSOR_INITIAL = "0"
DEFAULT_MAX_MESSAGE_SIZE = 4096


@dataclass(frozen=True, slots=True)
class CappingKey:
    """A counter key and its window in seconds."""

    key: str
    capping: int


@dataclass(frozen=True, slots=True)
class LimitKey:
    """A limiter key and its window in seconds."""

    key: str
    window: int


@dataclass(frozen=True, slots=True)
class RateLimitResult:
    allowed: bool
    current: int
    remaining: int
    reset_after: int


@dataclass(frozen=True, slots=True)
class LimitEvent:
    key: str
    window: int
    current: int
    reset_after: int


@dataclass(frozen=True, slots=True)
class QuotaAcquireResult:
    acquired: bool
    allocated: int
    used: int
    remaining: int
    expires_after: int


@dataclass(frozen=True, slots=True)
class QuotaClientInfo:
    client_id: str
    amount: int
    expires_at: int


@dataclass(frozen=True, slots=True)
class QuotaInfo:
    limit: int
    used: int
    remaining: int
    clients: tuple[QuotaClientInfo, ...]


@dataclass(frozen=True, slots=True)
class ScanKey:
    key: str
    window: int


@dataclass(frozen=True, slots=True)
class ScanResult:
    cursor: str
    keys: tuple[ScanKey, ...]


@dataclass(frozen=True, slots=True)
class QuotaEvent:
    event: str
    name: str
    client_id: str
    amount: int
    used: int
    remaining: int
    expires_at: int


class InspectSection(StrEnum):
    """INSPECT report section. SUMMARY means calling INSPECT without an argument."""

    SUMMARY = ""
    ALL = "ALL"
    WAL = "WAL"
    DUMP = "DUMP"
    REPL = "REPL"
    ENGINE = "ENGINE"
    STREAMS = "STREAMS"


_TLS_MIN_VERSIONS = {
    "": ssl.TLSVersion.TLSv1_2,
    "1.2": ssl.TLSVersion.TLSv1_2,
    "1.3": ssl.TLSVersion.TLSv1_3,
}


@dataclass(frozen=True, slots=True)
class TLSConfig:
    """Client side of TLS, mirroring the server's ``tls`` config block."""

    ca_file: str = ""
    cert_file: str = ""
    key_file: str = ""
    server_name: str = ""
    skip_verify: bool = False
    min_version: str = ""

    def build_context(self) -> ssl.SSLContext:
        """Build an ``ssl.SSLContext`` from file paths."""
        if bool(self.cert_file) != bool(self.key_file):
            raise ValueError("cert_file and key_file must be set together")

        minimum = _TLS_MIN_VERSIONS.get(self.min_version)
        if minimum is None:
            raise ValueError(f"unknown tls min version: {self.min_version!r}")

        context = ssl.create_default_context(
            ssl.Purpose.SERVER_AUTH,
            cafile=self.ca_file or None,
        )
        context.minimum_version = minimum

        if self.skip_verify:
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE

        if self.cert_file:
            context.load_cert_chain(self.cert_file, self.key_file)

        return context
