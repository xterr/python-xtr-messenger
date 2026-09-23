"""The wireup integration: two symbols, and what they do."""

from __future__ import annotations

import inspect
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Annotated, TypeVar, final
from uuid import UUID, uuid4

import pytest
import wireup
from wireup import AsyncContainer, Inject, Injected, injectable

from message_bus import (
    Envelope,
    HandlersLocator,
    MessageBusConfig,
    MessageBusFactory,
    MessageBusInterface,
    TransportConfig,
    WorkerFactory,
    as_message,
    as_message_handler,
)
from message_bus.integration.wireup import setup
from message_bus.transport.in_memory import InMemoryTransportFactory

pytestmark = pytest.mark.anyio

T = TypeVar("T")

opened: list[str] = []
closed: list[str] = []
sessions: list[str] = []
singletons: list[UUID] = []
handled: list[UUID] = []
unfilled: list[UUID] = []
envelopes: list[UUID] = []


@as_message(name="test.wireup.ingest.v1")
@dataclass(frozen=True, slots=True)
class IngestDocument:
    document_id: UUID


@injectable
@final
class Metrics:
    def __init__(self) -> None:
        self.identifier = uuid4()


@injectable(lifetime="scoped")
async def session() -> AsyncIterator[str]:
    """One per message, released when the message finishes."""
    made = str(uuid4())
    opened.append(made)
    try:
        yield made
    finally:
        closed.append(made)


@as_message_handler(IngestDocument)
async def ingest(message: IngestDocument, db: Injected[str], m: Injected[Metrics]) -> None:
    handled.append(message.document_id)
    sessions.append(db)
    singletons.append(m.identifier)


@as_message_handler(IngestDocument)
async def audit(message: IngestDocument) -> None:
    """Asks the container for nothing, and must still work."""
    unfilled.append(message.document_id)


@injectable
def bus_config(dsn: Annotated[str, Inject(config="dsn")]) -> MessageBusConfig:
    return MessageBusConfig(
        transports={"jobs": TransportConfig(dsn)},
        routing={IngestDocument: "jobs"},
    )


@pytest.fixture(autouse=True)
def _reset() -> None:
    for recorded in (opened, closed, sessions, singletons, handled, unfilled, envelopes):
        recorded.clear()


def a_config() -> MessageBusConfig:
    return MessageBusConfig(
        transports={"jobs": TransportConfig("in-memory://")},
        routing={IngestDocument: "jobs"},
    )


def a_container() -> AsyncContainer:
    """A container that knows about dependencies, and nothing about a bus."""
    container = wireup.create_async_container(injectables=[Metrics, session])
    setup(container)
    return container


async def drain(count: int = 1) -> None:
    """Publish ``count`` messages and let a worker handle them."""
    config, factories = a_config(), [InMemoryTransportFactory()]
    bus = MessageBusFactory(config, factories).bus()
    for _ in range(count):
        _ = await bus.dispatch(IngestDocument(document_id=uuid4()))
    await WorkerFactory(config, factories).worker(["jobs"]).run()


async def test_a_handler_receives_what_the_container_provides() -> None:
    container = a_container()

    await drain()

    assert len(handled) == 1
    assert len(sessions) == 1
    await container.close()


async def test_a_scoped_dependency_is_one_per_message_and_released() -> None:
    """The reason to wire a bus through a container: a transaction per message."""
    container = a_container()

    await drain(count=3)

    assert len(set(sessions)) == 3
    assert opened == closed != []
    await container.close()


async def test_a_singleton_stays_shared_by_every_message() -> None:
    """Scoping describes what a message should not share, not what a handler is."""
    container = a_container()

    await drain(count=3)

    assert len(set(singletons)) == 1
    await container.close()


async def test_a_handler_asking_for_nothing_is_untouched() -> None:
    container = a_container()

    await drain(count=2)

    assert len(unfilled) == 2
    await container.close()


def test_container_parameters_are_hidden_from_the_bus_automatically() -> None:
    """No decorator of ours: registration recognises wireup's annotation.

    Otherwise it would reject the handler for declaring a parameter the bus
    cannot pass.
    """
    assert tuple(inspect.signature(ingest).parameters) == ("message",)


def test_a_handler_may_still_ask_for_the_envelope_alongside() -> None:
    registry = HandlersLocator()

    async def with_envelope(
        message: IngestDocument,
        envelope: Envelope,
        db: Injected[str],
    ) -> None:
        del db, envelope
        envelopes.append(message.document_id)

    descriptor = registry.register(IngestDocument, with_envelope)

    assert descriptor.wants_envelope is True


async def test_setup_is_safe_to_call_twice() -> None:
    container = a_container()
    setup(container)

    await drain()

    assert len(handled) == 1
    await container.close()


async def test_a_publisher_needs_no_handlers_at_all() -> None:
    """Routing describes where a message goes, not who handles it."""
    container = wireup.create_async_container(injectables=[])
    setup(container, HandlersLocator())

    bus = MessageBusFactory(a_config(), [InMemoryTransportFactory()]).bus()
    sent = await bus.dispatch(IngestDocument(document_id=uuid4()))

    assert sent is not None
    await container.close()


async def test_a_bus_the_container_hands_out_is_six_lines_you_write() -> None:
    """There is no helper for this, and it does not need one."""
    container = wireup.create_async_container(injectables=[Metrics, session])

    @injectable
    def message_bus(config: MessageBusConfig, c: AsyncContainer) -> MessageBusInterface:
        setup(c)
        return MessageBusFactory(config, [InMemoryTransportFactory()]).bus()

    wired = wireup.create_async_container(
        injectables=[
            Metrics,
            session,
            message_bus,
            wireup.instance(a_config(), as_type=MessageBusConfig),
        ],
    )
    bus = await wired.get(MessageBusInterface)

    assert bus is not None
    _ = await bus.dispatch(IngestDocument(document_id=uuid4()))
    await wired.close()
    await container.close()
