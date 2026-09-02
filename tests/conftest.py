"""Shared test fixtures."""

from __future__ import annotations

import ssl
from collections.abc import Iterator
from pathlib import Path

import pytest
import trustme

from fq.types import TLSConfig


@pytest.fixture(scope="session")
def ca() -> trustme.CA:
    return trustme.CA()


@pytest.fixture(scope="session")
def ca_file(ca: trustme.CA, tmp_path_factory: pytest.TempPathFactory) -> Path:
    path = tmp_path_factory.mktemp("tls") / "ca.pem"
    ca.cert_pem.write_to_path(str(path))

    return path


@pytest.fixture
def server_ssl_context(ca: trustme.CA) -> Iterator[ssl.SSLContext]:
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    certificate = ca.issue_cert("localhost", "127.0.0.1")
    with certificate.private_key_and_cert_chain_pem.tempfile() as path:
        context.load_cert_chain(path)

        yield context


@pytest.fixture
def client_tls_config(ca_file: Path) -> TLSConfig:
    return TLSConfig(ca_file=str(ca_file), server_name="localhost")
