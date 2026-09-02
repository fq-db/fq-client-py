import pytest

from fq._chunked import ChunkAssembler
from fq.errors import CorruptedResponseError, InstanceStateError


def test_single_frame_response_needs_no_assembly() -> None:
    assembler = ChunkAssembler()

    assert assembler.feed(b'ok|{"section":"WAL"}') == b'ok|{"section":"WAL"}'


def test_chunks_are_concatenated_in_order() -> None:
    assembler = ChunkAssembler()

    assert assembler.feed(b'nxt|{"sec') is None
    assert assembler.feed(b'nxt|tion":') is None
    assert assembler.feed(b'ok|"WAL"}') == b'ok|{"section":"WAL"}'


def test_error_after_chunks_discards_the_body() -> None:
    assembler = ChunkAssembler()
    assembler.feed(b'nxt|{"partial"')

    with pytest.raises(InstanceStateError):
        assembler.feed(b"err|5002|inspect report too large")


def test_unknown_tag_is_corrupted() -> None:
    assembler = ChunkAssembler()

    with pytest.raises(CorruptedResponseError):
        assembler.feed(b"maybe|body")


def test_frame_without_a_delimiter_is_corrupted() -> None:
    assembler = ChunkAssembler()

    with pytest.raises(CorruptedResponseError):
        assembler.feed(b"nxt body")


def test_assembler_is_reusable_after_a_complete_response() -> None:
    assembler = ChunkAssembler()
    assembler.feed(b"nxt|first")
    assembler.feed(b"ok|-part")

    assert assembler.feed(b"ok|second") == b"ok|second"
