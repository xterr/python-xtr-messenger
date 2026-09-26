"""Handlers that also know how to handle a redispatch."""

from __future__ import annotations

import pytest

from tests.support.fakes import RecordingBus
from tests.support.messages import IngestDocument, ingest_document
from xtr_messenger import Envelope, HandlersLocator, MessageBusInterface, RedispatchMessage
from xtr_messenger.handler import RedispatchingHandlers

pytestmark = pytest.mark.anyio


def _never() -> MessageBusInterface:
    raise AssertionError("the publishing bus must not be built")


def test_other_messages_get_the_inner_handlers_only() -> None:
    inner = HandlersLocator()

    async def ingest(message: IngestDocument) -> None:
        del message

    descriptor = inner.register(IngestDocument, ingest)

    assert RedispatchingHandlers(inner, _never).handlers_for(IngestDocument) == (descriptor,)


def test_a_redispatch_gets_one_handler_when_the_inner_one_has_none() -> None:
    handlers = RedispatchingHandlers(HandlersLocator(), _never)

    [descriptor] = handlers.handlers_for(RedispatchMessage)

    assert descriptor.name == "RedispatchMessageHandler"
    assert RedispatchMessage in handlers.message_types()


def test_an_inner_handler_for_a_redispatch_wins_so_it_is_never_doubled() -> None:
    inner = HandlersLocator()

    async def own(message: RedispatchMessage) -> None:
        del message

    descriptor = inner.register(RedispatchMessage, own)

    assert RedispatchingHandlers(inner, _never).handlers_for(RedispatchMessage) == (descriptor,)


def test_registering_writes_to_the_inner_locator() -> None:
    inner = HandlersLocator()

    async def ingest(message: IngestDocument) -> None:
        del message

    _ = RedispatchingHandlers(inner, _never).register(IngestDocument, ingest)

    assert inner.message_types() == (IngestDocument,)


async def test_the_bus_is_built_on_the_first_redispatch_and_then_reused() -> None:
    bus = RecordingBus()
    built: list[MessageBusInterface] = []

    def publishing() -> MessageBusInterface:
        built.append(bus)
        return bus

    [descriptor] = RedispatchingHandlers(HandlersLocator(), publishing).handlers_for(
        RedispatchMessage
    )
    assert built == []

    _ = await descriptor.invoke(Envelope(RedispatchMessage(ingest_document())))
    _ = await descriptor.invoke(Envelope(RedispatchMessage(ingest_document())))

    assert len(built) == 1
    assert len(bus.dispatched) == 2
