"""Handling a redispatch by dispatching what it carries."""

from __future__ import annotations

from typing import final

import pytest
from typing_extensions import override

from tests.support.messages import ingest_document
from xtr_messenger import (
    AckReceiptStamp,
    DelayStamp,
    Envelope,
    HandledStamp,
    MessageBusInterface,
    ReceivedStamp,
    RedispatchMessage,
    StampInterface,
    TransportNamesStamp,
)
from xtr_messenger.handler import RedispatchMessageHandler

pytestmark = pytest.mark.anyio


@final
class AnsweringBus(MessageBusInterface):
    """Records dispatches and answers as if a handler returned ``answer``."""

    def __init__(self, answer: object = None) -> None:
        self.dispatched: list[Envelope] = []
        self._answer = answer

    @override
    async def dispatch(self, message: object, *stamps: StampInterface) -> Envelope:
        envelope = Envelope.wrap(message, stamps)
        self.dispatched.append(envelope)
        if self._answer is None:
            return envelope
        return envelope.with_stamps(HandledStamp("handler", self._answer))


async def test_named_transports_become_a_transport_names_stamp() -> None:
    bus = AnsweringBus()
    message = ingest_document()

    _ = await RedispatchMessageHandler(bus)(RedispatchMessage(message, ("high", "audit")))

    [envelope] = bus.dispatched
    assert envelope.message is message
    assert envelope.last(TransportNamesStamp) == TransportNamesStamp(("high", "audit"))


async def test_no_names_leave_routing_to_the_table() -> None:
    bus = AnsweringBus()

    _ = await RedispatchMessageHandler(bus)(RedispatchMessage(ingest_document()))

    assert bus.dispatched[0].last(TransportNamesStamp) is None


async def test_in_process_stamps_are_stripped_and_sendable_ones_kept() -> None:
    bus = AnsweringBus()
    carried = Envelope(ingest_document()).with_stamps(
        ReceivedStamp("jobs"), AckReceiptStamp(1), DelayStamp(5)
    )

    _ = await RedispatchMessageHandler(bus)(RedispatchMessage(carried))

    assert bus.dispatched[0].stamps == (DelayStamp(5),)


async def test_it_returns_what_the_carried_message_s_handler_returned() -> None:
    result = await RedispatchMessageHandler(AnsweringBus(answer=7))(
        RedispatchMessage(ingest_document())
    )

    assert result == 7


async def test_it_returns_nothing_when_the_carried_message_was_only_sent() -> None:
    result = await RedispatchMessageHandler(AnsweringBus())(RedispatchMessage(ingest_document()))

    assert result is None
