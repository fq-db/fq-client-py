"""Assembly of a response the server split across several frames.

A long response arrives as a sequence of ``nxt|<part>`` frames terminated by
exactly one ``ok|`` or ``err|``. A response that fits one frame passes through
the assembler unchanged.
"""

from __future__ import annotations

from fq._responses import RESP_DELIMITER, TAG_ERR, TAG_NXT, TAG_OK, parse_error, split_response
from fq.errors import CorruptedResponseError


class ChunkAssembler:
    """A state machine accumulating a response body from frames."""

    def __init__(self) -> None:
        self._body = bytearray()

    def feed(self, frame: bytes) -> bytes | None:
        """Feed one frame.

        Returns ``None`` while the response is incomplete, and the whole
        response shaped ``ok|<body>`` once the terminating frame arrives.
        """
        tag, data = split_response(frame)

        if tag == TAG_NXT:
            self._body.extend(data)

            return None

        if tag == TAG_OK:
            self._body.extend(data)
            body = bytes(self._body)
            self._body.clear()

            return TAG_OK + RESP_DELIMITER + body

        if tag == TAG_ERR:
            self._body.clear()

            raise parse_error(data)

        raise CorruptedResponseError(f"unknown frame tag: {tag!r}")
