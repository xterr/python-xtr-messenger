"""Building what a worker process runs, from configuration and factories."""

from __future__ import annotations

from typing import TYPE_CHECKING, final

import pytest
from typing_extensions import override
from xtr_event_dispatcher import EventDispatcher
from xtr_logging import Logger, TestHandler
from xtr_logging_contracts import Level

from tests.support.fakes import RecordingBus, RecordingMiddleware, StubReceiver
from tests.support.messages import IngestDocument, ingest_document
from xtr_messenger import (
    Envelope,
    HandlersLocator,
    IncompatibleReceiversError,
    MessageBusConfig,
    NotConsumableError,
    ReceivedStamp,
    SenderInterface,
    TransportConfig,
    TransportFactoryInterface,
    TransportInterface,
    UnknownMiddlewareError,
    UnknownTransportError,
    Worker,
    WorkerFactory,
    WorkerInterface,
    WorkerProvidingInterface,
    as_message_handler,
)
from xtr_messenger.event import WorkerMessageReceivedEvent

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterable, Mapping

    from xtr_event_dispatcher_contracts import EventDispatcherInterface

    from xtr_messenger import Dsn, MessageBusInterface

pytestmark = pytest.mark.anyio


@final
class FakeTransport(TransportInterface):
    """A whole transport whose receive half yields a fixed backlog."""

    def __init__(self, backlog: Iterable[Envelope]) -> None:
        self._backlog = tuple(backlog)
        self.acked: list[Envelope] = []

    @override
    async def send(self, envelope: Envelope) -> Envelope:
        return envelope

    @override
    async def get(self) -> AsyncIterator[Envelope]:
        for envelope in self._backlog:
            yield envelope

    @override
    async def ack(self, envelope: Envelope) -> None:
        self.acked.append(envelope)

    @override
    async def reject(self, envelope: Envelope) -> None:
        del envelope


@final
class WholeTransportFactory(TransportFactoryInterface):
    """Builds the ``whole://`` transports it was handed, keyed by name."""

    def __init__(self, transports: Mapping[str, FakeTransport]) -> None:
        self._transports = transports

    @override
    def supports(self, dsn: Dsn) -> bool:
        return dsn.scheme == "whole"

    @override
    def create(self, group: Mapping[str, TransportConfig]) -> Mapping[str, SenderInterface]:
        return {name: self._transports[name] for name in group}


@final
class FakeWorker(WorkerInterface):
    """A worker only good for being recognised — never run in these tests."""

    @override
    async def run(self) -> None: ...

    @override
    def stop(self) -> None: ...


@final
class OwnWorkerFactory(TransportFactoryInterface, WorkerProvidingInterface):
    """An adapter whose broker owns the loop; records the bus it is given."""

    def __init__(self) -> None:
        self.captured_bus: MessageBusInterface | None = None
        self.captured_dispatcher: EventDispatcherInterface | None = None
        self.built = FakeWorker()

    @override
    def supports(self, dsn: Dsn) -> bool:
        return dsn.scheme == "own"

    @override
    def create(self, group: Mapping[str, TransportConfig]) -> Mapping[str, SenderInterface]:
        del group
        return {}

    @override
    def worker(
        self,
        group: Mapping[str, TransportConfig],
        bus: MessageBusInterface,
        *,
        event_dispatcher: EventDispatcherInterface | None = None,
    ) -> WorkerInterface:
        del group
        self.captured_bus = bus
        self.captured_dispatcher = event_dispatcher
        return self.built


@final
class SendOnlySender(SenderInterface):
    """A transport that publishes and cannot be consumed."""

    @override
    async def send(self, envelope: Envelope) -> Envelope:
        return envelope


@final
class SendOnlyFactory(TransportFactoryInterface):
    """An adapter that forgot to say how its transport is consumed."""

    @override
    def supports(self, dsn: Dsn) -> bool:
        return dsn.scheme == "send-only"

    @override
    def create(self, group: Mapping[str, TransportConfig]) -> Mapping[str, SenderInterface]:
        return dict.fromkeys(group, SendOnlySender())


async def test_a_whole_transport_is_driven_by_the_library_worker() -> None:
    """A factory that builds a whole transport gets a library Worker; running
    it dispatches the backlog into the bus, where the handler picks it up."""
    seen: list[IngestDocument] = []
    handlers = HandlersLocator()

    @as_message_handler(IngestDocument, handlers)
    async def handle(message: IngestDocument) -> None:
        seen.append(message)

    first, second = ingest_document(), ingest_document()
    factory = WholeTransportFactory({"t": FakeTransport([Envelope(first), Envelope(second)])})
    config = MessageBusConfig(transports={"t": TransportConfig("whole://")})

    worker = WorkerFactory(config, [factory], handlers=handlers).worker(["t"])
    assert isinstance(worker, Worker)
    await worker.run()

    assert seen == [first, second]


async def test_several_receivers_on_one_connection_drain_through_one_loop() -> None:
    seen: list[IngestDocument] = []
    handlers = HandlersLocator()

    @as_message_handler(IngestDocument, handlers)
    async def handle(message: IngestDocument) -> None:
        seen.append(message)

    from_a, from_b = ingest_document(), ingest_document()
    factory = WholeTransportFactory(
        {
            "a": FakeTransport([Envelope(from_a)]),
            "b": FakeTransport([Envelope(from_b)]),
        },
    )
    config = MessageBusConfig(
        transports={"a": TransportConfig("whole://"), "b": TransportConfig("whole://")},
    )

    worker = WorkerFactory(config, [factory], handlers=handlers).worker(["a", "b"])
    await worker.run()

    assert seen == [from_a, from_b]


async def test_a_worker_providing_factory_returns_its_own_worker_over_a_handling_bus() -> None:
    """The adapter's worker is returned as-is, and the bus it captured handles
    from the locator passed to the factory — the one place handlers live."""
    seen: list[IngestDocument] = []
    handlers = HandlersLocator()

    @as_message_handler(IngestDocument, handlers)
    async def handle(message: IngestDocument) -> None:
        seen.append(message)

    adapter = OwnWorkerFactory()
    config = MessageBusConfig(transports={"jobs": TransportConfig("own://")})

    worker = WorkerFactory(config, [adapter], handlers=handlers).worker(["jobs"])

    assert worker is adapter.built
    assert adapter.captured_bus is not None
    message = ingest_document()
    _ = await adapter.captured_bus.dispatch(message, ReceivedStamp("jobs"))
    assert seen == [message]


async def test_the_bus_given_to_the_factory_is_the_one_passed_to_the_adapter() -> None:
    adapter = OwnWorkerFactory()
    own_bus = RecordingBus()
    config = MessageBusConfig(transports={"jobs": TransportConfig("own://")})

    _ = WorkerFactory(config, [adapter], bus=own_bus).worker(["jobs"])

    assert adapter.captured_bus is own_bus


def test_asking_for_an_unknown_transport_names_what_is_configured() -> None:
    config = MessageBusConfig(transports={"high": TransportConfig("sync://")})

    with pytest.raises(UnknownTransportError) as excinfo:
        _ = WorkerFactory(config).worker(["nope"])

    assert excinfo.value.names == ("nope",)
    assert excinfo.value.known == ("high",)


def test_a_send_only_transport_that_brings_no_worker_is_refused() -> None:
    """Neither consumable nor self-consuming leaves nothing to run."""
    config = MessageBusConfig(transports={"t": TransportConfig("send-only://")})

    with pytest.raises(NotConsumableError, match="WorkerProvidingInterface"):
        _ = WorkerFactory(config, [SendOnlyFactory()]).worker(["t"])


async def test_configured_middleware_runs_on_every_collected_message_before_handling() -> None:
    seen: list[IngestDocument] = []
    order: list[str] = []
    handlers = HandlersLocator()

    @as_message_handler(IngestDocument, handlers)
    async def handle(message: IngestDocument) -> None:
        order.append("handled")
        seen.append(message)

    first, second = ingest_document(), ingest_document()
    factory = WholeTransportFactory({"t": FakeTransport([Envelope(first), Envelope(second)])})
    config = MessageBusConfig(
        transports={"t": TransportConfig("whole://")},
        middleware=[RecordingMiddleware(order, "recorded")],
    )

    worker = WorkerFactory(config, [factory], handlers=handlers).worker(["t"])
    await worker.run()

    assert seen == [first, second]
    assert order == ["recorded", "handled", "recorded", "handled"]


async def test_configured_middleware_is_ignored_when_a_bus_is_given() -> None:
    """That bus is already composed, so nothing may be inserted into it."""
    order: list[str] = []
    adapter = OwnWorkerFactory()
    own_bus = RecordingBus()
    config = MessageBusConfig(
        transports={"jobs": TransportConfig("own://")},
        middleware=[RecordingMiddleware(order, "recorded")],
    )

    _ = WorkerFactory(config, [adapter], bus=own_bus).worker(["jobs"])

    assert adapter.captured_bus is own_bus
    assert order == []


async def test_default_middleware_off_leaves_handling_out_as_the_bus_does() -> None:
    """The worker dispatches into a bus, so the switch means the same there."""
    seen: list[IngestDocument] = []
    order: list[str] = []
    handlers = HandlersLocator()

    @as_message_handler(IngestDocument, handlers)
    async def handle(message: IngestDocument) -> None:
        seen.append(message)

    factory = WholeTransportFactory({"t": FakeTransport([Envelope(ingest_document())])})
    config = MessageBusConfig(
        transports={"t": TransportConfig("whole://")},
        middleware=[RecordingMiddleware(order, "recorded")],
        default_middleware=False,
    )

    await WorkerFactory(config, [factory], handlers=handlers).worker(["t"]).run()

    assert order == ["recorded"]
    assert seen == []


async def test_the_logging_name_writes_through_the_logger_the_factory_is_given() -> None:
    handler = TestHandler()
    handlers = HandlersLocator()

    @as_message_handler(IngestDocument, handlers)
    async def handle(message: IngestDocument) -> None:
        del message

    factory = WholeTransportFactory({"t": FakeTransport([Envelope(ingest_document())])})
    config = MessageBusConfig(transports={"t": TransportConfig("whole://")}, middleware=["logging"])

    await (
        WorkerFactory(config, [factory], handlers=handlers, logger=Logger("messenger", [handler]))
        .worker(["t"])
        .run()
    )

    assert handler.has_record("message handled", Level.INFO)


def test_a_middleware_name_nothing_is_registered_for_is_refused() -> None:
    config = MessageBusConfig(transports={"t": TransportConfig("whole://")}, middleware=["nope"])
    factory = WholeTransportFactory({"t": FakeTransport([])})

    with pytest.raises(UnknownMiddlewareError) as excinfo:
        _ = WorkerFactory(config, [factory]).worker(["t"])

    assert excinfo.value.name == "nope"
    assert excinfo.value.known == ("logging",)


async def test_workers_it_builds_announce_through_its_event_dispatcher() -> None:
    dispatcher = EventDispatcher()
    names: list[str] = []

    def record(event: WorkerMessageReceivedEvent) -> None:
        names.append(event.receiver_name)

    dispatcher.add_listener(WorkerMessageReceivedEvent, record)
    factory = WholeTransportFactory(
        {
            "a": FakeTransport([Envelope(ingest_document())]),
            "b": FakeTransport([Envelope(ingest_document())]),
        },
    )
    config = MessageBusConfig(
        transports={"a": TransportConfig("whole://"), "b": TransportConfig("whole://")},
    )
    workers = WorkerFactory(config, [factory], bus=RecordingBus(), event_dispatcher=dispatcher)

    await workers.worker(["a"]).run()
    await workers.worker(["a", "b"]).run()

    assert names == ["a", "a", "b"]


def test_a_worker_providing_factory_is_handed_the_event_dispatcher() -> None:
    adapter = OwnWorkerFactory()
    dispatcher = EventDispatcher()
    config = MessageBusConfig(transports={"jobs": TransportConfig("own://")})

    _ = WorkerFactory(config, [adapter], event_dispatcher=dispatcher).worker(["jobs"])

    assert adapter.captured_dispatcher is dispatcher


async def test_a_registered_receiver_is_consumed_by_its_name() -> None:
    seen: list[IngestDocument] = []
    handlers = HandlersLocator()

    @as_message_handler(IngestDocument, handlers)
    async def handle(message: IngestDocument) -> None:
        seen.append(message)

    message = ingest_document()
    receiver = StubReceiver([Envelope(message)])
    config = MessageBusConfig(transports={})

    worker = WorkerFactory(config, handlers=handlers, receivers={"generated": receiver})
    await worker.worker(["generated"]).run()

    assert seen == [message]
    assert receiver.acked != []


async def test_a_configured_transport_wins_over_a_registered_receiver_of_its_name() -> None:
    configured = FakeTransport([Envelope(ingest_document())])
    registered = StubReceiver([Envelope(ingest_document())])
    config = MessageBusConfig(transports={"a": TransportConfig("whole://")})
    factory = WholeTransportFactory({"a": configured})

    workers = WorkerFactory(config, [factory], bus=RecordingBus(), receivers={"a": registered})
    await workers.worker(["a"]).run()

    assert registered.acked == []


async def test_registered_receivers_and_configured_transports_drain_together_by_name() -> None:
    dispatcher = EventDispatcher()
    names: list[str] = []

    def record(event: WorkerMessageReceivedEvent) -> None:
        names.append(event.receiver_name)

    dispatcher.add_listener(WorkerMessageReceivedEvent, record)
    config = MessageBusConfig(transports={"a": TransportConfig("whole://")})
    factory = WholeTransportFactory({"a": FakeTransport([Envelope(ingest_document())])})
    registered = StubReceiver([Envelope(ingest_document())])
    workers = WorkerFactory(
        config,
        [factory],
        bus=RecordingBus(),
        event_dispatcher=dispatcher,
        receivers={"generated": registered},
    )

    await workers.worker(["generated", "a"]).run()

    assert names == ["generated", "a"]


def test_an_unknown_name_lists_configured_and_registered_names() -> None:
    config = MessageBusConfig(transports={"high": TransportConfig("sync://")})
    workers = WorkerFactory(config, receivers={"generated": StubReceiver()})

    with pytest.raises(UnknownTransportError) as caught:
        _ = workers.worker(["nope"])

    assert caught.value.known == ("high", "generated")


def test_a_transport_bringing_its_own_worker_cannot_share_it_with_a_receiver() -> None:
    config = MessageBusConfig(transports={"jobs": TransportConfig("own://")})
    workers = WorkerFactory(config, [OwnWorkerFactory()], receivers={"generated": StubReceiver()})

    with pytest.raises(IncompatibleReceiversError):
        _ = workers.worker(["jobs", "generated"])
