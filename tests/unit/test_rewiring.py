from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, final
from uuid import UUID, uuid4

import pytest
from typing_extensions import override

from message_bus import (
    Dsn,
    EncodedEnvelope,
    Envelope,
    HandlersLocator,
    InMemoryTransportFactory,
    JsonSerializer,
    MessageBus,
    MessageBusConfig,
    MessageBusFactory,
    NoSenderForMessageError,
    SenderInterface,
    SendersLocator,
    SendMessageMiddleware,
    SerializerInterface,
    SyncTransport,
    SyncTransportFactory,
    TransportConfig,
    TransportFactoryInterface,
    TransportInterface,
    as_message,
    as_message_handler,
    default_registry,
)

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Mapping


pytestmark = pytest.mark.anyio


@as_message(name="test.rewire.job.v1")
@dataclass(frozen=True, slots=True)
class RewireJob:
    job_id: UUID


@final
class CountingSerializer(SerializerInterface):
    def __init__(self) -> None:
        self._inner = JsonSerializer()
        self.encoded = 0

    @override
    def encode(self, envelope: Envelope) -> EncodedEnvelope:
        self.encoded += 1
        return self._inner.encode(envelope)

    @override
    def decode(self, encoded: EncodedEnvelope) -> Envelope:
        return self._inner.decode(encoded)


@final
class CountingSender(TransportInterface):
    """A transport written from outside the library, implementing both halves."""

    def __init__(self) -> None:
        self.count = 0

    @override
    async def send(self, envelope: Envelope) -> Envelope:
        self.count += 1
        return envelope

    @override
    async def get(self) -> AsyncIterator[Envelope]:
        for envelope in self._backlog():
            yield envelope

    @override
    async def ack(self, envelope: Envelope) -> None:
        del envelope

    @override
    async def reject(self, envelope: Envelope) -> None:
        del envelope

    def _backlog(self) -> tuple[Envelope, ...]:
        return ()


def a_job() -> RewireJob:
    return RewireJob(job_id=uuid4())


async def test_a_private_handlers_locator_replaces_the_global_one() -> None:
    private = HandlersLocator()
    seen: list[RewireJob] = []

    @as_message_handler(RewireJob, private)
    async def handle(message: RewireJob) -> None:
        seen.append(message)

    config = MessageBusConfig(
        transports={"sync": TransportConfig("sync://")},
        routing={RewireJob: "sync"},
    )
    bus = MessageBusFactory(config, [SyncTransportFactory(handlers=private)]).bus()

    await bus.dispatch(a_job())

    assert len(seen) == 1
    assert RewireJob not in default_registry().message_types()


async def test_the_in_memory_factory_uses_the_serializer_it_is_given() -> None:
    counting = CountingSerializer()
    config = MessageBusConfig(
        transports={"test": TransportConfig("in-memory://?serialize=true")},
        routing={RewireJob: "test"},
    )

    await (
        MessageBusFactory(config, [InMemoryTransportFactory(serializer=counting)])
        .bus()
        .dispatch(a_job())
    )

    assert counting.encoded == 1


async def test_a_sender_of_your_own_can_stand_in_for_a_transport() -> None:
    counting = CountingSender()
    locator = SendersLocator({RewireJob: "mine"}, {"mine": counting})

    await MessageBus([SendMessageMiddleware(locator)]).dispatch(a_job())

    assert counting.count == 1


async def test_a_bus_can_be_composed_without_the_factory_at_all() -> None:
    private = HandlersLocator()
    seen: list[RewireJob] = []

    @as_message_handler(RewireJob, private)
    async def handle(message: RewireJob) -> None:
        seen.append(message)

    locator = SendersLocator({RewireJob: "sync"}, {"sync": SyncTransport(private)})
    bus = MessageBus([SendMessageMiddleware(locator, require_sender=True)])

    await bus.dispatch(a_job())

    assert len(seen) == 1


async def test_the_factory_can_reject_an_unrouted_message() -> None:
    config = MessageBusConfig(transports={"sync": TransportConfig("sync://")})

    with pytest.raises(NoSenderForMessageError):
        await MessageBusFactory(config).bus(require_sender=True).dispatch(a_job())


async def test_a_factory_of_your_own_can_serve_a_scheme_the_library_does_not() -> None:
    config = MessageBusConfig(
        transports={"odd": TransportConfig("carrier-pigeon://")},
        routing={RewireJob: "odd"},
    )
    pigeons = _PigeonFactory()

    await MessageBusFactory(config, [pigeons]).bus().dispatch(a_job())

    assert pigeons.delivered == 1


@final
class _PigeonFactory(TransportFactoryInterface):
    def __init__(self) -> None:
        self._sender = CountingSender()

    @property
    def delivered(self) -> int:
        return self._sender.count

    @override
    def supports(self, dsn: Dsn) -> bool:
        return dsn.scheme == "carrier-pigeon"

    @override
    def create(self, group: Mapping[str, TransportConfig]) -> Mapping[str, SenderInterface]:
        return dict.fromkeys(group, self._sender)
