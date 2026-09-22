from __future__ import annotations

from uuid import uuid4

import pytest

from message_bus import (
    Envelope,
    HandledStamp,
    HandlersLocator,
    InMemoryTransport,
    JsonSerializer,
    MessageBus,
    MessageDecodingFailedError,
    SendersLocator,
    SendMessageMiddleware,
    SyncTransport,
)
from tests.conftest import AnalyseDocument, IngestDocument

pytestmark = pytest.mark.anyio


def ingest_message() -> IngestDocument:
    return IngestDocument(document_id=uuid4(), tenant_id=uuid4())


async def test_sync_transport_runs_the_handler_in_the_calling_task() -> None:
    seen: list[IngestDocument] = []
    registry = HandlersLocator()

    async def handle(message: object) -> None:
        assert isinstance(message, IngestDocument)
        seen.append(message)

    _ = registry.register(IngestDocument, handle)
    table = SendersLocator({IngestDocument: "sync"}, {"sync": SyncTransport(registry)})
    bus = MessageBus([SendMessageMiddleware(table)])
    message = ingest_message()

    await bus.dispatch(message)

    assert seen == [message]


async def test_sync_transport_records_which_handler_ran() -> None:
    registry = HandlersLocator()

    async def handle(message: object) -> None:
        del message

    _ = registry.register(IngestDocument, handle, name="ingest-handler")
    result = await SyncTransport(registry).send(Envelope(ingest_message()))

    assert result.last(HandledStamp) == HandledStamp("ingest-handler")


async def test_sync_transport_lets_handler_failures_reach_the_dispatcher() -> None:
    registry = HandlersLocator()

    async def handle(message: object) -> None:
        del message
        raise RuntimeError("handler exploded")

    _ = registry.register(IngestDocument, handle)
    table = SendersLocator({IngestDocument: "sync"}, {"sync": SyncTransport(registry)})
    bus = MessageBus([SendMessageMiddleware(table)])

    with pytest.raises(RuntimeError, match="handler exploded"):
        await bus.dispatch(ingest_message())


async def test_sync_transport_is_a_no_op_when_nothing_handles_the_message() -> None:
    table = SendersLocator({IngestDocument: "sync"}, {"sync": SyncTransport(HandlersLocator())})
    bus = MessageBus([SendMessageMiddleware(table)])

    result = await bus.dispatch(ingest_message())

    assert result.last(HandledStamp) is None


async def test_in_memory_transport_records_what_was_dispatched() -> None:
    transport = InMemoryTransport()
    first, second = ingest_message(), ingest_message()

    table = SendersLocator({IngestDocument: "async"}, {"async": transport})
    bus = MessageBus([SendMessageMiddleware(table)])
    await bus.dispatch(first)
    await bus.dispatch(second)

    assert transport.messages == (first, second)


async def test_in_memory_transport_can_be_cleared_between_assertions() -> None:
    transport = InMemoryTransport()
    table = SendersLocator({IngestDocument: "async"}, {"async": transport})
    bus = MessageBus([SendMessageMiddleware(table)])

    await bus.dispatch(ingest_message())
    transport.clear()

    assert transport.messages == ()


async def test_in_memory_transport_round_trips_through_the_serializer_when_asked() -> None:
    transport = InMemoryTransport(serializer=JsonSerializer())
    message = ingest_message()

    await transport.send(Envelope(message))

    assert transport.messages == (message,)


async def test_in_memory_transport_surfaces_a_message_that_cannot_be_serialized() -> None:
    transport = InMemoryTransport(serializer=JsonSerializer())

    with pytest.raises(MessageDecodingFailedError):
        await transport.send(Envelope("not a dataclass"))


async def test_handlers_registered_for_a_base_type_receive_subclasses() -> None:
    registry = HandlersLocator()

    async def handle(message: object) -> None:
        del message

    _ = registry.register(AnalyseDocument, handle)

    assert len(registry.handlers_for(AnalyseDocument)) == 1
    assert registry.message_types() == (AnalyseDocument,)
