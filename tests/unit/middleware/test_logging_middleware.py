"""One structured log record per dispatch, written after the chain has run."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, final

import pytest
from typing_extensions import override

from message_bus import (
    Envelope,
    LoggerInterface,
    LoggingMiddleware,
    MiddlewareInterface,
    SentStamp,
    StackInterface,
    TransportMessageIdStamp,
)
from message_bus.middleware import logging_middleware
from tests.support.messages import ingest_document

if TYPE_CHECKING:
    from message_bus import StampInterface

pytestmark = pytest.mark.anyio


@final
class RecordingLogger(LoggerInterface):
    """Records every ``(message, fields)`` it is asked to log."""

    def __init__(self) -> None:
        self.records: list[tuple[str, dict[str, str]]] = []

    @override
    def info(self, message: str, /, **fields: str) -> None:
        self.records.append((message, fields))


@final
class StampingTerminal(MiddlewareInterface):
    """Stamps the result — seeing those stamps proves the log ran afterwards."""

    def __init__(self, *stamps: StampInterface) -> None:
        self._stamps = stamps

    @override
    async def handle(self, envelope: Envelope, stack: StackInterface, /) -> Envelope:
        del stack
        return envelope.with_stamps(*self._stamps)


@final
class ExplodingTerminal(MiddlewareInterface):
    """Raises instead of returning — the failing layer owns reporting itself."""

    @override
    async def handle(self, envelope: Envelope, stack: StackInterface, /) -> Envelope:
        del envelope, stack
        raise RuntimeError("downstream boom")


@final
class OneStep(StackInterface):
    """A stack that hands back a single terminal middleware."""

    def __init__(self, terminal: MiddlewareInterface) -> None:
        self._terminal = terminal

    @override
    def next(self) -> MiddlewareInterface:
        return self._terminal


async def test_it_logs_the_message_type_with_no_extra_fields_when_none_apply() -> None:
    logger = RecordingLogger()

    _ = await LoggingMiddleware(logger).handle(
        Envelope(ingest_document()),
        OneStep(StampingTerminal()),
    )

    assert logger.records == [("message dispatched", {"message_type": "IngestDocument"})]


async def test_it_adds_the_transport_and_message_id_the_chain_stamped() -> None:
    """The stamps come from the terminal, so seeing them in the log proves the
    record is written after the rest of the chain has run."""
    logger = RecordingLogger()
    terminal = StampingTerminal(
        SentStamp("InMemoryTransport", "async"),
        TransportMessageIdStamp("id-1"),
    )

    _ = await LoggingMiddleware(logger).handle(Envelope(ingest_document()), OneStep(terminal))

    assert logger.records[0] == (
        "message dispatched",
        {"message_type": "IngestDocument", "transport": "async", "message_id": "id-1"},
    )


async def test_the_transport_is_taken_from_the_last_sent_stamp() -> None:
    logger = RecordingLogger()
    terminal = StampingTerminal(SentStamp("A", "primary"), SentStamp("B", "mirror"))

    _ = await LoggingMiddleware(logger).handle(Envelope(ingest_document()), OneStep(terminal))

    assert logger.records[0][1]["transport"] == "mirror"


async def test_it_returns_the_result_untouched() -> None:
    logger = RecordingLogger()
    stamp = SentStamp("InMemoryTransport", "async")
    envelope = Envelope(ingest_document())

    result = await LoggingMiddleware(logger).handle(envelope, OneStep(StampingTerminal(stamp)))

    assert result.stamps == (stamp,)


async def test_an_exception_from_the_chain_propagates_and_nothing_is_logged() -> None:
    logger = RecordingLogger()

    with pytest.raises(RuntimeError, match="downstream boom"):
        _ = await LoggingMiddleware(logger).handle(
            Envelope(ingest_document()),
            OneStep(ExplodingTerminal()),
        )

    assert logger.records == []


async def test_the_default_logger_writes_through_the_stdlib_logger(
    caplog: pytest.LogCaptureFixture,
) -> None:
    with caplog.at_level(logging.INFO, logger=logging_middleware.__name__):
        _ = await LoggingMiddleware().handle(
            Envelope(ingest_document()),
            OneStep(StampingTerminal()),
        )

    assert caplog.messages == ["message dispatched"]
