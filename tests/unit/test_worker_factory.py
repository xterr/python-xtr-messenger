"""Building what a worker process runs, from configuration and factories."""

from __future__ import annotations

from typing import TYPE_CHECKING, final

import pytest
from typing_extensions import override

from tests.support.fakes import RecordingBus
from tests.support.messages import IngestDocument, ingest_document
from xtr_messenger import (
    Envelope,
    HandlersLocator,
    MessageBusConfig,
    NotConsumableError,
    ReceivedStamp,
    SenderInterface,
    TransportConfig,
    TransportFactoryInterface,
    TransportInterface,
    UnknownTransportError,
    Worker,
    WorkerFactory,
    WorkerInterface,
    WorkerProvidingInterface,
    as_message_handler,
)

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterable, Mapping

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
    ) -> WorkerInterface:
        del group
        self.captured_bus = bus
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
