from __future__ import annotations

from uuid import uuid4

import pytest

from message_bus import (
    Envelope,
    HandlerSignatureError,
    HandlersLocator,
    MessageBus,
    SendersLocator,
    SendMessageMiddleware,
    SyncTransport,
    as_message_handler,
    default_registry,
)
from tests.conftest import IngestDocument

pytestmark = pytest.mark.anyio


def ingest_message() -> IngestDocument:
    return IngestDocument(document_id=uuid4(), tenant_id=uuid4())


def test_declaring_a_handler_needs_no_transport_and_no_broker() -> None:
    registry = HandlersLocator()

    @as_message_handler(IngestDocument, registry)
    async def ingest(message: IngestDocument) -> None:
        del message

    assert registry.message_types() == (IngestDocument,)


def test_the_decorator_returns_the_original_function_untouched() -> None:
    registry = HandlersLocator()

    async def ingest(message: IngestDocument) -> None:
        del message

    decorated = as_message_handler(IngestDocument, registry)(ingest)

    assert decorated is ingest


def test_a_handler_declared_without_a_registry_lands_in_the_default_one() -> None:
    @as_message_handler(IngestDocument)
    async def ingest(message: IngestDocument) -> None:
        del message

    assert IngestDocument in default_registry().message_types()


def test_a_handler_taking_only_the_message_is_accepted() -> None:
    registry = HandlersLocator()

    async def ingest(message: IngestDocument) -> None:
        del message

    assert registry.register(IngestDocument, ingest).wants_envelope is False


def test_a_handler_taking_the_envelope_is_accepted() -> None:
    registry = HandlersLocator()

    async def ingest(message: IngestDocument, envelope: Envelope) -> None:
        del message, envelope

    assert registry.register(IngestDocument, ingest).wants_envelope is True


def test_a_second_parameter_that_is_not_an_envelope_is_refused() -> None:
    registry = HandlersLocator()

    async def ingest(message: IngestDocument, extra: str) -> None:
        del message, extra

    with pytest.raises(HandlerSignatureError, match="annotated Envelope"):
        _ = registry.register(IngestDocument, ingest)


def test_a_third_parameter_is_refused() -> None:
    registry = HandlersLocator()

    async def ingest(message: IngestDocument, envelope: Envelope, extra: str) -> None:
        del message, envelope, extra

    with pytest.raises(HandlerSignatureError) as excinfo:
        _ = registry.register(IngestDocument, ingest)

    assert excinfo.value.parameters == ("message", "envelope", "extra")


def test_two_handlers_can_share_one_message_type() -> None:
    registry = HandlersLocator()

    @as_message_handler(IngestDocument, registry)
    async def first(message: IngestDocument) -> None:
        del message

    @as_message_handler(IngestDocument, registry)
    async def second(message: IngestDocument) -> None:
        del message

    assert len(registry.handlers_for(IngestDocument)) == 2


def test_bindings_expose_every_message_and_handler_pair() -> None:
    registry = HandlersLocator()

    @as_message_handler(IngestDocument, registry)
    async def ingest(message: IngestDocument) -> None:
        del message

    bindings = registry.bindings()

    assert len(bindings) == 1
    assert bindings[0][0] is IngestDocument


async def test_one_declaration_serves_the_sync_transport() -> None:
    seen: list[IngestDocument] = []
    registry = HandlersLocator()

    @as_message_handler(IngestDocument, registry)
    async def ingest(message: IngestDocument) -> None:
        seen.append(message)

    table = SendersLocator({IngestDocument: "sync"}, {"sync": SyncTransport(registry)})
    bus = MessageBus([SendMessageMiddleware(table)])
    message = ingest_message()

    await bus.dispatch(message)

    assert seen == [message]


async def test_the_sync_transport_passes_the_envelope_when_asked() -> None:
    seen: list[Envelope] = []
    registry = HandlersLocator()

    @as_message_handler(IngestDocument, registry)
    async def ingest(message: IngestDocument, envelope: Envelope) -> None:
        del message
        seen.append(envelope)

    table = SendersLocator({IngestDocument: "sync"}, {"sync": SyncTransport(registry)})
    bus = MessageBus([SendMessageMiddleware(table)])

    await bus.dispatch(ingest_message())

    assert isinstance(seen[0], Envelope)
