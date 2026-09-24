"""One structured log record per dispatch, written after the chain has run."""

from __future__ import annotations

from typing import TYPE_CHECKING, final

import pytest
from typing_extensions import override
from xtr_logging import Level, Logger, TestHandler

from tests.support.messages import ingest_document
from xtr_messenger import (
    Envelope,
    LoggingMiddleware,
    MiddlewareInterface,
    SentStamp,
    StackInterface,
    TransportMessageIdStamp,
)

if TYPE_CHECKING:
    from xtr_messenger import StampInterface

pytestmark = pytest.mark.anyio


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


def recording() -> tuple[Logger, TestHandler]:
    """A logger writing only to a handler that keeps what it is given."""
    handler = TestHandler()
    return Logger("app", [handler]), handler


async def test_it_logs_the_message_type_with_no_extra_context_when_none_applies() -> None:
    logger, handler = recording()

    _ = await LoggingMiddleware(logger).handle(
        Envelope(ingest_document()),
        OneStep(StampingTerminal()),
    )

    assert [(record.message, dict(record.context)) for record in handler.records] == [
        ("message dispatched", {"message_type": "IngestDocument"}),
    ]


async def test_it_logs_at_info() -> None:
    logger, handler = recording()

    _ = await LoggingMiddleware(logger).handle(
        Envelope(ingest_document()),
        OneStep(StampingTerminal()),
    )

    assert handler.has_record("message dispatched", Level.INFO)


async def test_it_adds_the_transport_and_message_id_the_chain_stamped() -> None:
    """The stamps come from the terminal, so seeing them in the log proves the
    record is written after the rest of the chain has run."""
    logger, handler = recording()
    terminal = StampingTerminal(
        SentStamp("InMemoryTransport", "async"),
        TransportMessageIdStamp("id-1"),
    )

    _ = await LoggingMiddleware(logger).handle(Envelope(ingest_document()), OneStep(terminal))

    assert dict(handler.records[0].context) == {
        "message_type": "IngestDocument",
        "transport": "async",
        "message_id": "id-1",
    }


async def test_the_transport_is_taken_from_the_last_sent_stamp() -> None:
    logger, handler = recording()
    terminal = StampingTerminal(SentStamp("A", "primary"), SentStamp("B", "mirror"))

    _ = await LoggingMiddleware(logger).handle(Envelope(ingest_document()), OneStep(terminal))

    assert handler.records[0].context["transport"] == "mirror"


async def test_it_returns_the_result_untouched() -> None:
    logger, _ = recording()
    stamp = SentStamp("InMemoryTransport", "async")
    envelope = Envelope(ingest_document())

    result = await LoggingMiddleware(logger).handle(envelope, OneStep(StampingTerminal(stamp)))

    assert result.stamps == (stamp,)


async def test_an_exception_from_the_chain_propagates_and_nothing_is_logged() -> None:
    logger, handler = recording()

    with pytest.raises(RuntimeError, match="downstream boom"):
        _ = await LoggingMiddleware(logger).handle(
            Envelope(ingest_document()),
            OneStep(ExplodingTerminal()),
        )

    assert handler.records == ()


async def test_without_a_logger_it_discards_the_record_and_still_returns_the_result() -> None:
    stamp = SentStamp("InMemoryTransport", "async")

    result = await LoggingMiddleware().handle(
        Envelope(ingest_document()),
        OneStep(StampingTerminal(stamp)),
    )

    assert result.stamps == (stamp,)
