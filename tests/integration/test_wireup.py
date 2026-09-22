"""The wireup integration: handlers built by a container, per message."""

from __future__ import annotations

import sys
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import TypeVar, final
from uuid import UUID, uuid4

import pytest
import wireup
from wireup import AsyncContainer, injectable

from message_bus import (
    Envelope,
    HandlerSignatureError,
    MessageBusConfig,
    MessageBusInterface,
    TransportConfig,
    Worker,
    WorkerInterface,
    as_message,
)
from message_bus.handler import HandlersLocatorInterface
from message_bus.integration.wireup import ContainerHandlersLocator, make_injectables
from message_bus.stamp import ErrorDetailsStamp
from message_bus.transport.in_memory import InMemoryTransport, InMemoryTransportFactory

pytestmark = pytest.mark.anyio


@as_message(name="test.wireup.order.v1")
@dataclass(frozen=True, slots=True)
class OrderPlaced:
    order_id: UUID


@final
class Session:
    """Stands in for anything a message should get its own of."""

    def __init__(self, identifier: UUID) -> None:
        self.identifier = identifier
        self.committed: list[UUID] = []


opened: list[UUID] = []
closed: list[UUID] = []
handled: list[UUID] = []


@injectable(lifetime="scoped")
async def session() -> AsyncIterator[Session]:
    made = Session(uuid4())
    opened.append(made.identifier)
    try:
        yield made
    finally:
        closed.append(made.identifier)


@injectable(lifetime="scoped")
@final
class OrderHandler:
    def __init__(self, db: Session) -> None:
        self._db = db

    async def __call__(self, message: OrderPlaced) -> None:
        self._db.committed.append(message.order_id)
        handled.append(self._db.identifier)


@injectable(lifetime="scoped")
@final
class EnvelopeHandler:
    def __init__(self, db: Session) -> None:
        self._db = db

    async def __call__(self, message: OrderPlaced, envelope: Envelope) -> None:
        del envelope
        handled.append(message.order_id)


@pytest.fixture(autouse=True)
def _reset() -> None:
    opened.clear()
    closed.clear()
    handled.clear()


def a_config() -> MessageBusConfig:
    return MessageBusConfig(
        transports={"orders": TransportConfig("in-memory://")},
        routing={OrderPlaced: "orders"},
    )


def a_container(
    handlers: dict[type, type],
    transports: tuple[str, ...] = (),
) -> AsyncContainer:
    return wireup.create_async_container(
        injectables=[
            sys.modules[__name__],
            *make_injectables(
                a_config(),
                handlers=handlers,
                transports=transports,
                factories=[InMemoryTransportFactory()],
            ),
        ],
    )


async def test_the_container_provides_a_bus() -> None:
    container = a_container({OrderPlaced: OrderHandler})

    bus = await resolve(container, MessageBusInterface)

    assert isinstance(bus, MessageBusInterface)
    await container.close()


async def test_a_worker_is_registered_only_when_transports_are_named() -> None:
    """A publishing process has no worker to provide."""
    publisher = a_container({OrderPlaced: OrderHandler})

    with pytest.raises(Exception, match="WorkerInterface"):
        _ = await resolve(publisher, WorkerInterface)

    await publisher.close()


async def test_a_worker_is_provided_when_transports_are_named() -> None:
    container = a_container({OrderPlaced: OrderHandler}, transports=("orders",))

    worker = await resolve(container, WorkerInterface)

    assert isinstance(worker, Worker)
    await container.close()


async def test_a_handler_receives_what_the_container_injects() -> None:
    container = a_container({OrderPlaced: OrderHandler}, transports=("orders",))
    bus = await resolve(container, MessageBusInterface)
    order_id = uuid4()

    _ = await bus.dispatch(OrderPlaced(order_id=order_id))
    await (await resolve(container, WorkerInterface)).run()

    assert len(handled) == 1
    await container.close()


async def test_every_message_gets_its_own_scoped_dependency() -> None:
    """The reason to wire a bus through a container at all.

    A session, a transaction, a request id — one per message, and released
    when the message is finished with.
    """
    container = a_container({OrderPlaced: OrderHandler}, transports=("orders",))
    bus = await resolve(container, MessageBusInterface)
    for _ in range(3):
        _ = await bus.dispatch(OrderPlaced(order_id=uuid4()))

    await (await resolve(container, WorkerInterface)).run()

    assert len(set(handled)) == 3
    assert opened == closed != []
    await container.close()


async def test_a_handler_may_still_ask_for_the_envelope() -> None:
    container = a_container({OrderPlaced: EnvelopeHandler}, transports=("orders",))
    bus = await resolve(container, MessageBusInterface)

    _ = await bus.dispatch(OrderPlaced(order_id=uuid4()))
    await (await resolve(container, WorkerInterface)).run()

    assert len(handled) == 1
    await container.close()


async def test_the_locator_looks_up_by_class_not_by_instance() -> None:
    container = a_container({OrderPlaced: OrderHandler})
    locator = await resolve(container, HandlersLocatorInterface)

    assert isinstance(locator, ContainerHandlersLocator)
    assert [d.name for d in locator.handlers_for(OrderPlaced)] == ["OrderHandler"]
    assert locator.message_types() == (OrderPlaced,)
    await container.close()


def test_a_handler_with_a_shape_the_bus_cannot_call_is_refused_while_wiring() -> None:
    """Checked from the class, so it fails before any message arrives."""

    @final
    class Wrong:
        async def __call__(self, message: OrderPlaced, extra: str) -> None: ...

    locator = ContainerHandlersLocator(None, {OrderPlaced: Wrong})  # pyright: ignore[reportArgumentType]

    with pytest.raises(HandlerSignatureError, match="Wrong"):
        _ = locator.handlers_for(OrderPlaced)


def test_a_class_that_cannot_be_called_is_refused() -> None:
    @final
    class NotCallable:
        pass

    locator = ContainerHandlersLocator(None, {OrderPlaced: NotCallable})  # pyright: ignore[reportArgumentType]

    with pytest.raises(HandlerSignatureError, match="NotCallable"):
        _ = locator.handlers_for(OrderPlaced)


async def test_a_handler_the_container_cannot_build_rejects_with_the_reason() -> None:
    """Bound as a handler but never registered as an injectable.

    wireup cannot catch this while validating: the locator resolves by class
    when a message arrives, which is runtime behaviour rather than a graph
    edge. So it surfaces per message — and because the worker refuses to die
    on one message, it surfaces as a rejection carrying why.
    """

    @final
    class Unregistered:
        async def __call__(self, message: OrderPlaced) -> None: ...

    factory = InMemoryTransportFactory()
    container = wireup.create_async_container(
        injectables=[
            sys.modules[__name__],
            *make_injectables(
                a_config(),
                handlers={OrderPlaced: Unregistered},
                transports=("orders",),
                factories=[factory],
            ),
        ],
    )
    bus = await resolve(container, MessageBusInterface)
    _ = await bus.dispatch(OrderPlaced(order_id=uuid4()))

    await (await resolve(container, WorkerInterface)).run()

    transport = factory.create({"orders": TransportConfig("in-memory://")})["orders"]
    assert isinstance(transport, InMemoryTransport)
    details = transport.rejected[0].last(ErrorDetailsStamp)
    assert details is not None
    assert "Unregistered" in details.exception_message
    await container.close()


T = TypeVar("T")


async def resolve(container: AsyncContainer, what: type[T]) -> T:
    """Return what the container provides, refusing None."""
    got = await container.get(what)
    assert got is not None
    return got
