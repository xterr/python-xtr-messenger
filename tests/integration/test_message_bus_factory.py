"""A bus built by the factory, with real transports wired end to end."""

from __future__ import annotations

import pytest

from tests.support.fakes import RecordingMiddleware
from tests.support.messages import IngestDocument, ingest_document
from xtr_messenger import (
    HandlersLocator,
    InMemoryTransport,
    InMemoryTransportFactory,
    MessageBusConfig,
    MessageBusFactory,
    NoHandlerForMessageError,
    TransportConfig,
    as_message_handler,
)

pytestmark = pytest.mark.anyio


async def test_a_sync_transport_reaches_a_handler_from_the_given_locator() -> None:
    seen: list[IngestDocument] = []
    handlers = HandlersLocator()

    @as_message_handler(IngestDocument, handlers)
    async def ingest(message: IngestDocument) -> None:
        seen.append(message)

    config = MessageBusConfig(
        transports={"sync": TransportConfig("sync://")},
        routing={IngestDocument: "sync"},
    )
    message = ingest_document()

    _ = await MessageBusFactory(config, handlers=handlers).bus().dispatch(message)

    assert seen == [message]


async def test_a_sync_routed_message_with_no_handler_fails_loudly() -> None:
    """Routing a message to be handled here, with nothing to handle it, is a
    mistake rather than a silent no-op."""
    config = MessageBusConfig(
        transports={"sync": TransportConfig("sync://")},
        routing={IngestDocument: "sync"},
    )

    with pytest.raises(NoHandlerForMessageError, match="IngestDocument"):
        _ = (
            await MessageBusFactory(config, handlers=HandlersLocator())
            .bus()
            .dispatch(
                ingest_document(),
            )
        )


async def test_an_in_memory_transport_records_what_was_dispatched() -> None:
    factory = InMemoryTransportFactory()
    config = MessageBusConfig(
        transports={"test": TransportConfig("in-memory://")},
        routing={IngestDocument: "test"},
    )
    message = ingest_document()

    _ = await MessageBusFactory(config, [factory]).bus().dispatch(message)

    recorder = factory.create(config.transports)["test"]
    assert isinstance(recorder, InMemoryTransport)
    assert recorder.messages == (message,)


async def test_configured_middleware_runs_before_the_send() -> None:
    order: list[str] = []
    config = MessageBusConfig(
        transports={"test": TransportConfig("in-memory://")},
        routing={IngestDocument: "test"},
        middleware=[RecordingMiddleware(order)],
    )

    _ = (
        await MessageBusFactory(config, handlers=HandlersLocator())
        .bus()
        .dispatch(ingest_document())
    )

    assert order == ["middleware"]
