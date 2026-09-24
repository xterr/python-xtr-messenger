"""A bus built by the factory, with real transports wired end to end."""

from __future__ import annotations

from typing import TYPE_CHECKING, final

import pytest
from typing_extensions import override

from tests.support.messages import IngestDocument, ingest_document
from xtr_messenger import (
    HandlersLocator,
    InMemoryTransport,
    InMemoryTransportFactory,
    MessageBusConfig,
    MessageBusFactory,
    MiddlewareInterface,
    NoHandlerForMessageError,
    TransportConfig,
    as_message_handler,
)

if TYPE_CHECKING:
    from xtr_messenger import Envelope, StackInterface

pytestmark = pytest.mark.anyio


@final
class RecordingMiddleware(MiddlewareInterface):
    """Notes that it ran, then continues the chain."""

    def __init__(self, calls: list[str]) -> None:
        self._calls = calls

    @override
    async def handle(self, envelope: Envelope, stack: StackInterface, /) -> Envelope:
        self._calls.append("middleware")
        return await stack.next().handle(envelope, stack)


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


async def test_middleware_passed_to_bus_runs_before_the_send() -> None:
    order: list[str] = []
    config = MessageBusConfig(
        transports={"test": TransportConfig("in-memory://")},
        routing={IngestDocument: "test"},
    )

    _ = await (
        MessageBusFactory(config, handlers=HandlersLocator())
        .bus([RecordingMiddleware(order)])
        .dispatch(ingest_document())
    )

    assert order == ["middleware"]
