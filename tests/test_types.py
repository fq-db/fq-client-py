import dataclasses
import ssl

import pytest

from fq.types import (
    CappingKey,
    InspectSection,
    LimitKey,
    QuotaClientInfo,
    QuotaInfo,
    TLSConfig,
)


def test_keys_are_frozen() -> None:
    key = CappingKey("user_42", 60)

    with pytest.raises(dataclasses.FrozenInstanceError):
        key.key = "other"  # type: ignore[misc]


def test_keys_are_comparable_and_hashable() -> None:
    assert CappingKey("user_42", 60) == CappingKey("user_42", 60)
    assert len({LimitKey("a", 60), LimitKey("a", 60)}) == 1


def test_quota_info_holds_clients_as_a_tuple() -> None:
    info = QuotaInfo(10, 4, 6, (QuotaClientInfo("client-a", 4, 0),))

    assert info.clients[0].client_id == "client-a"


def test_summary_section_is_the_empty_string() -> None:
    assert InspectSection.SUMMARY.value == ""
    assert InspectSection.WAL.value == "WAL"


def test_empty_tls_config_builds_a_verifying_context() -> None:
    context = TLSConfig().build_context()

    assert context.verify_mode is ssl.CERT_REQUIRED
    assert context.minimum_version is ssl.TLSVersion.TLSv1_2


def test_skip_verify_disables_verification() -> None:
    context = TLSConfig(skip_verify=True).build_context()

    assert context.check_hostname is False
    assert context.verify_mode is ssl.CERT_NONE


def test_min_version_1_3_is_honoured() -> None:
    context = TLSConfig(min_version="1.3").build_context()

    assert context.minimum_version is ssl.TLSVersion.TLSv1_3


def test_unknown_min_version_is_rejected() -> None:
    with pytest.raises(ValueError, match="unknown tls min version"):
        TLSConfig(min_version="1.1").build_context()


def test_half_a_keypair_is_rejected() -> None:
    with pytest.raises(ValueError, match="together"):
        TLSConfig(cert_file="client.crt").build_context()
