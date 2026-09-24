"""Every way wireup can hand a handler what it needs, through the one call."""

from __future__ import annotations

from collections.abc import AsyncIterator, Iterator
from dataclasses import dataclass
from typing import Annotated, Protocol, TypeVar, final
from uuid import UUID, uuid4

import pytest
import wireup
from taskiq import InMemoryBroker
from wireup import AsyncContainer, Inject, Injected, injectable
from wireup.errors import WireupError

from xtr_messenger import (
    Envelope,
    HandlersLocator,
    MessageBus,
    MessageBusConfig,
    MessageBusInterface,
    TransportConfig,
    UnregisteredHandlerError,
    Worker,
    WorkerInterface,
    as_message,
    as_message_handler,
)
from xtr_messenger.bridge.taskiq import TaskiqSender, bind_bus
from xtr_messenger.bridge.taskiq.broker import forget_started
from xtr_messenger.bridge.taskiq.taskiq_worker import TaskiqWorker
from xtr_messenger.integration.wireup import injectables
from xtr_messenger.middleware import HandleMessageMiddleware, SendMessageMiddleware
from xtr_messenger.stamp import HandledStamp
from xtr_messenger.transport.in_memory import InMemoryTransportFactory
from xtr_messenger.transport.sender import SendersLocator
from xtr_messenger.transport.sync import SyncTransportFactory

pytestmark = pytest.mark.anyio

T = TypeVar("T")

HANDLERS = HandlersLocator()

# ─── what the container knows about ──────────────────────────────


@final
class Session:
    def __init__(self) -> None:
        self.identifier = uuid4()
        self.open = True


sessions: list[Session] = []


@injectable(lifetime="scoped")
async def session() -> AsyncIterator[Session]:
    """One per handler call, closed when it finishes."""
    made = Session()
    sessions.append(made)
    try:
        yield made
    finally:
        made.open = False


@injectable
@final
class Metrics:
    def __init__(self) -> None:
        self.identifier = uuid4()


class Clock(Protocol):
    def now(self) -> str: ...


@injectable(as_type=Clock)
@final
class FixedClock:
    def now(self) -> str:
        return "noon"


@final
class Region:
    def __init__(self, name: str) -> None:
        self.name = name


@injectable(qualifier="primary")
def primary_region() -> Region:
    return Region("eu-west")


@injectable(qualifier="replica")
def replica_region() -> Region:
    return Region("eu-central")


@final
class Unregistered:
    pass


# ─── messages, one per scenario ──────────────────────────────────


@as_message(name="test.wireup.ingest.v1")
@dataclass(frozen=True, slots=True)
class Ingest:
    identifier: UUID


@as_message(name="test.wireup.plain.v1")
@dataclass(frozen=True, slots=True)
class Plain:
    identifier: UUID


@as_message(name="test.wireup.stamped.v1")
@dataclass(frozen=True, slots=True)
class Stamped:
    identifier: UUID


@as_message(name="test.wireup.configured.v1")
@dataclass(frozen=True, slots=True)
class Configured:
    identifier: UUID


@as_message(name="test.wireup.failing.v1")
@dataclass(frozen=True, slots=True)
class Failing:
    identifier: UUID


@as_message(name="test.wireup.audited.v1")
@dataclass(frozen=True, slots=True)
class Audited:
    identifier: UUID


@as_message(name="test.wireup.invoice.v1")
@dataclass(frozen=True, slots=True)
class Invoice:
    identifier: UUID


@as_message(name="test.wireup.inline.v1")
@dataclass(frozen=True, slots=True)
class Inline:
    identifier: UUID


@as_message(name="test.wireup.late.v1")
@dataclass(frozen=True, slots=True)
class Late:
    identifier: UUID


# ─── handlers, declared the way an application would ─────────────

seen: list[tuple[str, tuple[object, ...]]] = []


def calls(kind: str) -> list[tuple[object, ...]]:
    return [arguments for name, arguments in seen if name == kind]


@as_message_handler(Ingest, HANDLERS)
async def ingest(message: Ingest, db: Injected[Session], metrics: Injected[Metrics]) -> None:
    seen.append(("ingest", (message, db, metrics)))


@as_message_handler(Plain, HANDLERS)
async def plain(message: Plain) -> None:
    seen.append(("plain", (message,)))


@as_message_handler(Stamped, HANDLERS)
async def stamped(message: Stamped, envelope: Envelope, db: Injected[Session]) -> None:
    seen.append(("stamped", (message, envelope, db)))


@as_message_handler(Configured, HANDLERS)
async def configured(
    message: Configured,
    tenant: Annotated[str, Inject(config="tenant")],
    region: Annotated[Region, Inject(qualifier="replica")],
    clock: Injected[Clock],
) -> None:
    seen.append(("configured", (message, tenant, region.name, clock.now())))


@as_message_handler(Failing, HANDLERS)
async def failing(message: Failing, db: Injected[Session]) -> None:
    seen.append(("failing", (message, db)))
    raise RuntimeError("the handler failed")


@final
class Auditor:
    """A handler that is an object, with what it needs on ``__call__``."""

    async def __call__(self, message: Audited, db: Injected[Session]) -> None:
        seen.append(("audited", (message, db)))


_ = HANDLERS.register(Audited, Auditor())


constructed: list[object] = []


@as_message_handler(Invoice, HANDLERS)
@final
class IssueInvoice:
    """A handler class: built once by the container, fresh dependencies per call.

    No ``@injectable`` — the integration registers it, as a singleton.
    """

    def __init__(self, metrics: Metrics) -> None:
        constructed.append(self)
        self.metrics = metrics

    async def __call__(self, message: Invoice, envelope: Envelope, db: Injected[Session]) -> None:
        seen.append(("invoice", (message, envelope, self, db)))


@as_message_handler(Inline, HANDLERS)
async def inline(message: Inline, db: Injected[Session]) -> None:
    seen.append(("inline", (message, db)))


@injectable
def bus_config_from_settings(dsn: Annotated[str, Inject(config="dsn")]) -> MessageBusConfig:
    """How an application reads its configuration from the container instead."""
    return MessageBusConfig(transports={"jobs": TransportConfig(dsn)}, routing={"*": "jobs"})


SERVICES: list[object] = [
    session,
    Metrics,
    FixedClock,
    primary_region,
    replica_region,
]

CONFIG = MessageBusConfig(
    transports={"jobs": TransportConfig("in-memory://"), "inline": TransportConfig("sync://")},
    routing={Inline: "inline", "*": "jobs"},
)


@pytest.fixture(autouse=True)
def _reset() -> None:
    seen.clear()
    sessions.clear()
    constructed.clear()


def a_container(
    config: MessageBusConfig | None = CONFIG,
    *,
    transports: tuple[str, ...] = ("jobs",),
    handlers: HandlersLocator = HANDLERS,
    services: list[object] | None = None,
) -> AsyncContainer:
    return wireup.create_async_container(
        injectables=[
            *(SERVICES if services is None else services),
            *injectables(
                config,
                transports=transports,
                factories=[InMemoryTransportFactory(), SyncTransportFactory()],
                handlers=handlers,
            ),
        ],
        config={"tenant": "acme", "dsn": "in-memory://"},
    )


async def resolve(container: AsyncContainer, what: type[T]) -> T:
    got = await container.get(what)
    assert got is not None
    return got


async def drain(container: AsyncContainer, *messages: object) -> None:
    """Publish ``messages`` through the container's bus, then let its worker run."""
    bus = await resolve(container, MessageBusInterface)
    for message in messages:
        _ = await bus.dispatch(message)
    await (await resolve(container, WorkerInterface)).run()


@pytest.fixture
async def container() -> AsyncIterator[AsyncContainer]:
    made = a_container()
    yield made
    await made.close()


# ─── what the container provides ─────────────────────────────────


async def test_the_container_provides_the_bus_the_worker_and_the_config(
    container: AsyncContainer,
) -> None:
    assert await resolve(container, MessageBusConfig) is CONFIG
    assert isinstance(await resolve(container, MessageBusInterface), MessageBus)
    assert isinstance(await resolve(container, WorkerInterface), Worker)


async def test_a_publishing_process_is_given_no_worker() -> None:
    publisher = a_container(transports=())

    assert await resolve(publisher, MessageBusInterface) is not None
    with pytest.raises(WireupError, match="WorkerInterface"):
        _ = await publisher.get(WorkerInterface)
    await publisher.close()


async def test_the_config_may_come_from_the_container_itself() -> None:
    wired = a_container(None, services=[*SERVICES, bus_config_from_settings])

    await drain(wired, Plain(uuid4()))

    assert list((await resolve(wired, MessageBusConfig)).transports) == ["jobs"]
    assert len(calls("plain")) == 1
    await wired.close()


# ─── function handlers ───────────────────────────────────────────


async def test_a_handler_receives_what_the_container_provides(container: AsyncContainer) -> None:
    message = Ingest(uuid4())

    await drain(container, message)

    [(received, db, metrics)] = calls("ingest")
    assert received == message
    assert isinstance(db, Session)
    assert isinstance(metrics, Metrics)


async def test_a_scoped_dependency_is_one_per_message_and_released(
    container: AsyncContainer,
) -> None:
    await drain(container, Ingest(uuid4()), Ingest(uuid4()), Ingest(uuid4()))

    assert len({id(db) for _, db, _ in calls("ingest")}) == 3
    assert sessions != []
    assert not any(made.open for made in sessions)


async def test_a_singleton_is_shared_by_every_message(container: AsyncContainer) -> None:
    await drain(container, Ingest(uuid4()), Ingest(uuid4()))

    assert len({id(metrics) for _, _, metrics in calls("ingest")}) == 1


async def test_a_scoped_dependency_is_released_when_the_handler_fails(
    container: AsyncContainer,
) -> None:
    await drain(container, Failing(uuid4()))

    [(_, db)] = calls("failing")
    assert isinstance(db, Session)
    assert db.open is False


async def test_a_handler_asking_for_nothing_runs_as_declared(container: AsyncContainer) -> None:
    await drain(container, Plain(uuid4()))

    [descriptor] = HANDLERS.handlers_for(Plain)
    assert descriptor.call is plain
    assert len(calls("plain")) == 1


async def test_a_handler_gets_the_envelope_alongside_what_it_injects(
    container: AsyncContainer,
) -> None:
    message = Stamped(uuid4())

    await drain(container, message)

    [(received, envelope, db)] = calls("stamped")
    assert received == message
    assert isinstance(envelope, Envelope)
    assert envelope.message == message
    assert isinstance(db, Session)


async def test_config_values_qualifiers_and_interfaces_are_injected(
    container: AsyncContainer,
) -> None:
    await drain(container, Configured(uuid4()))

    [(_, tenant, region, now)] = calls("configured")
    assert (tenant, region, now) == ("acme", "eu-central", "noon")


# ─── handlers that are objects, or classes ───────────────────────


async def test_a_callable_object_has_its_call_filled(container: AsyncContainer) -> None:
    await drain(container, Audited(uuid4()))

    [(_, db)] = calls("audited")
    assert isinstance(db, Session)


async def test_a_handler_class_is_built_once_and_shared_by_every_message(
    container: AsyncContainer,
) -> None:
    """Thousands of messages a second must not mean thousands of constructions."""
    first, second = Invoice(uuid4()), Invoice(uuid4())

    await drain(container, first, second)

    built = calls("invoice")
    assert [message for message, _, _, _ in built] == [first, second]
    [handler] = constructed
    assert isinstance(handler, IssueInvoice)
    assert all(called is handler for _, _, called, _ in built)
    assert handler.metrics is await resolve(container, Metrics)


async def test_a_handler_class_gets_fresh_dependencies_on_every_call(
    container: AsyncContainer,
) -> None:
    await drain(container, Invoice(uuid4()), Invoice(uuid4()))

    built = calls("invoice")
    sessions_used = [db for _, _, _, db in built]
    assert all(isinstance(db, Session) and db.open is False for db in sessions_used)
    assert sessions_used[0] is not sessions_used[1]
    assert all(isinstance(envelope, Envelope) for _, envelope, _, _ in built)


async def test_a_handler_class_asking_its_constructor_for_a_scoped_dependency_is_refused() -> None:
    """A singleton cannot hold what lives for one message; that belongs on __call__."""
    registry = HandlersLocator()

    @as_message_handler(Plain, registry)
    @final
    class HoldsSession:
        def __init__(self, db: Session) -> None:
            self.db = db

        async def __call__(self, message: Plain) -> None:
            del message

    with pytest.raises(WireupError, match="scoped"):
        _ = a_container(handlers=registry)


async def test_a_handler_class_declared_after_the_container_was_built_is_refused() -> None:
    registry = HandlersLocator()
    wired = a_container(handlers=registry)

    @as_message_handler(Plain, registry)
    @final
    class TooLate:
        async def __call__(self, message: Plain) -> None:
            del message

    with pytest.raises(UnregisteredHandlerError, match="TooLate"):
        _ = await wired.get(MessageBusInterface)
    await wired.close()


# ─── every road a message takes to its handler ───────────────────


async def test_the_sync_transport_injects_at_dispatch() -> None:
    publisher = a_container(transports=())
    bus = await resolve(publisher, MessageBusInterface)

    sent = await bus.dispatch(Inline(uuid4()))

    [(_, db)] = calls("inline")
    assert isinstance(db, Session)
    assert sent.last(HandledStamp) is not None
    await publisher.close()


@pytest.fixture
def broker() -> Iterator[InMemoryBroker]:
    made = InMemoryBroker(await_inplace=True)
    yield made
    forget_started(made)


async def test_handlers_bound_to_a_taskiq_broker_are_injected(
    container: AsyncContainer,
    broker: InMemoryBroker,
) -> None:
    """The route a broker bringing its own worker takes: tasks dispatching into a bus."""
    _ = await resolve(container, WorkerInterface)
    _ = bind_bus(broker, MessageBus([HandleMessageMiddleware(HANDLERS)]))
    senders = SendersLocator({Ingest: "q"}, {"q": TaskiqSender(broker)})

    _ = await MessageBus([SendMessageMiddleware(senders)]).dispatch(Ingest(uuid4()))

    [(_, db, _)] = calls("ingest")
    assert isinstance(db, Session)


# ─── wiring ──────────────────────────────────────────────────────


async def test_the_bus_and_the_worker_share_discovered_transports() -> None:
    """Otherwise the worker consumes from a transport the bus never published to."""
    wired = wireup.create_async_container(
        injectables=[
            *SERVICES,
            *injectables(
                MessageBusConfig(
                    transports={"jobs": TransportConfig("in-memory://")}, routing={"*": "jobs"}
                ),
                transports=["jobs"],
                handlers=HANDLERS,
            ),
        ],
        config={"tenant": "acme"},
    )

    await drain(wired, Ingest(uuid4()))

    assert len(calls("ingest")) == 1
    await wired.close()


async def test_a_broker_bringing_its_own_worker_still_supplies_it() -> None:
    wired = wireup.create_async_container(
        injectables=injectables(
            MessageBusConfig(transports={"jobs": TransportConfig("amqp://guest:guest@localhost/")}),
            transports=["jobs"],
            handlers=HandlersLocator(),
        ),
    )

    assert isinstance(await resolve(wired, WorkerInterface), TaskiqWorker)
    await wired.close()


async def test_an_override_reaches_the_handler(container: AsyncContainer) -> None:
    fake = Metrics()

    with container.override({Metrics: fake}):
        await drain(container, Ingest(uuid4()))

    [(_, _, metrics)] = calls("ingest")
    assert metrics is fake


async def test_wiring_another_container_rebinds_every_handler() -> None:
    """As a test suite does, one container per test."""
    first, second = a_container(), a_container()

    await drain(first, Ingest(uuid4()))
    await drain(second, Ingest(uuid4()))

    [(_, _, from_first), (_, _, from_second)] = calls("ingest")
    assert from_first is await resolve(first, Metrics)
    assert from_second is await resolve(second, Metrics)
    await first.close()
    await second.close()


async def test_wiring_again_does_not_stack(container: AsyncContainer) -> None:
    """The bus and the worker each wire, and a handler still runs once, in one scope."""
    await drain(container, Ingest(uuid4()))

    assert len(calls("ingest")) == 1
    assert len(sessions) == 1


async def test_a_handler_asking_for_what_the_container_lacks_is_refused_while_wiring() -> None:
    registry = HandlersLocator()

    @as_message_handler(Plain, registry)
    async def needy(message: Plain, missing: Injected[Unregistered]) -> None:
        del message, missing

    wired = a_container(handlers=registry)

    with pytest.raises(WireupError, match="Unregistered"):
        _ = await wired.get(MessageBusInterface)
    await wired.close()


async def test_a_handler_declared_after_wiring_is_wired_too() -> None:
    """A module imported late still gets its handlers' dependencies."""
    registry = HandlersLocator()
    wired = a_container(handlers=registry)
    _ = await resolve(wired, WorkerInterface)

    @as_message_handler(Late, registry)
    async def late(message: Late, db: Injected[Session]) -> None:
        seen.append(("late", (message, db)))

    await drain(wired, Late(uuid4()))

    [(_, db)] = calls("late")
    assert isinstance(db, Session)
    await wired.close()
