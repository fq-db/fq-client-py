from fq import _commands as commands
from fq.types import CappingKey, InspectSection, LimitKey


def test_hello_without_a_token() -> None:
    assert commands.hello(None).payload == b"HELLO 1"


def test_hello_carries_the_token_inline() -> None:
    assert commands.hello("s3cret").payload == b"HELLO 1 AUTH s3cret"


def test_token_with_base64_padding_is_passed_verbatim() -> None:
    assert commands.hello("YWJj+ZA==").payload == b"HELLO 1 AUTH YWJj+ZA=="


def test_counter_commands() -> None:
    key = CappingKey("user_42", 60)

    assert commands.incr(key).payload == b"INCR user_42 60"
    assert commands.get(key).payload == b"GET user_42 60"
    assert commands.watch(key).payload == b"WATCH user_42 60"
    assert commands.delete(key).payload == b"DEL user_42 60"


def test_mdelete_lists_every_key_and_window() -> None:
    keys = [CappingKey("a", 60), CappingKey("b", 30)]

    assert commands.mdelete(keys).payload == b"MDEL a 60 b 30"


def test_rate_limit_commands() -> None:
    key = LimitKey("user_42", 60)

    assert commands.rlimit_fixed_window(key, 100).payload == b"RLIMIT FW user_42 100 60"
    assert commands.rlimit_sliding_window(key, 100).payload == b"RLIMIT SW user_42 100 60"
    assert commands.rlimit_token_bucket(key, 100, 10).payload == b"RLIMIT TB user_42 100 10 60"


def test_quota_commands() -> None:
    assert commands.quota_set("plan", 10).payload == b"QUOTA SET plan 10"
    assert commands.quota_set_n("plan", 10, 5).payload == b"QUOTA SETN plan 10 5"
    assert commands.quota_acquire("plan", 4, "c1").payload == b"QUOTA ACQ plan 4 c1"
    assert commands.quota_acquire_n("plan", "c1").payload == b"QUOTA ACQN plan c1"
    assert commands.quota_acquire_lease("plan", 10, 4, "c1").payload == b"QUOTA ACQL plan 10 4 c1"
    assert commands.quota_release("plan", "c1").payload == b"QUOTA REL plan c1"
    assert commands.quota_delete("plan").payload == b"QUOTA DEL plan"
    assert commands.quota_info("plan").payload == b"QUOTA INF plan"


def test_quota_ttl_is_appended_when_given() -> None:
    assert commands.quota_acquire("plan", 4, "c1", ttl=30).payload == b"QUOTA ACQ plan 4 c1 30"
    assert commands.quota_acquire_n("plan", "c1", ttl=30).payload == b"QUOTA ACQN plan c1 30"
    assert (
        commands.quota_acquire_lease("plan", 10, 4, "c1", ttl=30).payload
        == b"QUOTA ACQL plan 10 4 c1 30"
    )


def test_zero_ttl_is_still_sent() -> None:
    assert commands.quota_acquire("plan", 4, "c1", ttl=0).payload == b"QUOTA ACQ plan 4 c1 0"


def test_scan_commands() -> None:
    assert commands.scan(100, "0").payload == b"SCAN 0 100"
    assert commands.pscan("user_", 100, "17").payload == b"PSCAN user_ 17 100"


def test_empty_cursor_is_normalised_to_zero() -> None:
    assert commands.scan(100, "").payload == b"SCAN 0 100"
    assert commands.pscan("user_", 100, "").payload == b"PSCAN user_ 0 100"


def test_maintenance_commands() -> None:
    assert commands.flushdb().payload == b"FLUSHDB"
    assert commands.truncate().payload == b"TRUNCATE"


def test_inspect_summary_has_no_argument() -> None:
    assert commands.inspect(InspectSection.SUMMARY).payload == b"INSPECT"


def test_inspect_section_is_appended() -> None:
    assert commands.inspect(InspectSection.WAL).payload == b"INSPECT WAL"


def test_stream_commands() -> None:
    assert commands.stream().payload == b"STREAM"
    assert commands.pstream("user_").payload == b"PSTREAM user_"
    assert commands.qstream().payload == b"QSTREAM"
    assert commands.qpstream("plan_").payload == b"QPSTREAM plan_"


def test_read_only_commands_are_marked() -> None:
    key = CappingKey("user_42", 60)

    assert commands.get(key).read_only is True
    assert commands.watch(key).read_only is True
    assert commands.scan(10, "0").read_only is True
    assert commands.pscan("p", 10, "0").read_only is True
    assert commands.quota_info("plan").read_only is True
    assert commands.inspect(InspectSection.ALL).read_only is True
    assert commands.hello(None).read_only is True


def test_mutating_commands_are_marked() -> None:
    key = CappingKey("user_42", 60)

    assert commands.incr(key).read_only is False
    assert commands.delete(key).read_only is False
    assert commands.mdelete([key]).read_only is False
    assert commands.rlimit_fixed_window(LimitKey("k", 60), 1).read_only is False
    assert commands.quota_acquire("plan", 1, "c1").read_only is False
    assert commands.quota_set("plan", 1).read_only is False
    assert commands.quota_release("plan", "c1").read_only is False
    assert commands.quota_delete("plan").read_only is False
    assert commands.flushdb().read_only is False
    assert commands.truncate().read_only is False
