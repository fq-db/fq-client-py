"""Bringing up a real fq server in Docker for integration tests."""

from __future__ import annotations

import shutil
import subprocess
import time
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

import pytest

from fq.errors import FQError
from fq.sync.client import Client

IMAGE = "ghcr.io/fq-db/fq:latest"

ADMIN_TOKEN = "admin-token-for-tests"
RW_TOKEN = "rw-token-for-tests"
RO_TOKEN = "ro-token-for-tests"

CONFIG = """
network:
  address: ":1945"
  max_connections: 100
  max_message_size: 4KB
  idle_timeout: 10m
  auth:
    tokens:
      - { role: admin, token_env: FQ_ADMIN_TOKEN }
      - { role: rw, token_env: FQ_RW_TOKEN }
      - { role: ro, token_env: FQ_RO_TOKEN }
persistence:
  mode: memory
engine:
  type: in_memory
  partitions: 4
  key_index: true
  clean_interval: 5m
  limit_event_queue_capacity: 64
logging:
  level: info
"""


@dataclass(frozen=True)
class FQInstance:
    address: str
    admin_token: str = ADMIN_TOKEN
    rw_token: str = RW_TOKEN
    ro_token: str = RO_TOKEN


def docker_available() -> bool:
    if shutil.which("docker") is None:
        return False

    return subprocess.run(["docker", "info"], capture_output=True, check=False).returncode == 0


requires_docker = pytest.mark.skipif(not docker_available(), reason="docker is not available")


def _write_config(directory: Path) -> Path:
    path = directory / "config.yml"
    path.write_text(CONFIG)
    path.chmod(0o644)

    return path


def _start(config: Path) -> tuple[str, str]:
    container = subprocess.run(
        [
            "docker",
            "run",
            "-d",
            "--rm",
            "-p",
            "127.0.0.1::1945",
            "-e",
            f"FQ_ADMIN_TOKEN={ADMIN_TOKEN}",
            "-e",
            f"FQ_RW_TOKEN={RW_TOKEN}",
            "-e",
            f"FQ_RO_TOKEN={RO_TOKEN}",
            "-v",
            f"{config}:/etc/fq/config.yml:ro",
            IMAGE,
        ],
        capture_output=True,
        check=True,
        text=True,
    ).stdout.strip()

    mapping = subprocess.run(
        ["docker", "port", container, "1945/tcp"],
        capture_output=True,
        check=True,
        text=True,
    ).stdout.strip()

    port = mapping.splitlines()[0].rsplit(":", 1)[1]

    return container, f"127.0.0.1:{port}"


def _wait_ready(address: str, container: str, attempts: int = 60) -> None:
    for _ in range(attempts):
        try:
            client = Client(address, pool_size=1, token=ADMIN_TOKEN, connect_timeout=1.0)
        except (FQError, OSError):
            time.sleep(0.5)

            continue

        client.close()

        return

    logs = subprocess.run(
        ["docker", "logs", container],
        capture_output=True,
        check=False,
        text=True,
    )

    raise RuntimeError(f"fq did not become ready at {address}\n{logs.stdout}\n{logs.stderr}")


def _stop(container: str) -> None:
    subprocess.run(["docker", "rm", "-f", container], capture_output=True, check=False)


def _instance(tmp_path: Path, name: str) -> Iterator[FQInstance]:
    directory = tmp_path / name
    directory.mkdir(parents=True, exist_ok=True)
    config = _write_config(directory)

    container, address = _start(config)
    try:
        _wait_ready(address, container)

        yield FQInstance(address=address)
    finally:
        _stop(container)


@pytest.fixture(scope="module")
def fq_container(tmp_path_factory: pytest.TempPathFactory) -> Iterator[FQInstance]:
    yield from _instance(tmp_path_factory.mktemp("fq"), "single")


@pytest.fixture(scope="module")
def fq_cluster(tmp_path_factory: pytest.TempPathFactory) -> Iterator[list[FQInstance]]:
    root = tmp_path_factory.mktemp("fq-cluster")
    first = _instance(root, "shard-0")
    second = _instance(root, "shard-1")
    instances = [next(first), next(second)]
    try:
        yield instances
    finally:
        for generator in (first, second):
            next(generator, None)
