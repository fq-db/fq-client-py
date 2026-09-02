import fq


def test_clients_are_exported() -> None:
    assert fq.Client is not None
    assert fq.AsyncClient is not None
    assert fq.ShardedClient is not None
    assert fq.AsyncShardedClient is not None


def test_types_are_exported() -> None:
    key = fq.CappingKey("k", 60)
    limit = fq.LimitKey("k", 60)

    assert key.capping == 60
    assert limit.window == 60
    assert fq.InspectSection.WAL.value == "WAL"
    assert fq.TLSConfig().skip_verify is False
    assert fq.ReconnectPolicy().max_attempts is None


def test_errors_are_exported() -> None:
    assert issubclass(fq.AuthenticationFailedError, fq.AuthError)
    assert issubclass(fq.AuthError, fq.ProtocolError)
    assert issubclass(fq.ProtocolError, fq.FQError)
    permission_denied: int = 3001

    assert permission_denied == fq.ErrorCode.PERMISSION_DENIED


def test_default_sharding_function_is_exported() -> None:
    assert 0 <= fq.fnv1a_shard("k", 4) < 4


def test_everything_in_dunder_all_is_importable() -> None:
    for name in fq.__all__:
        assert hasattr(fq, name), name
