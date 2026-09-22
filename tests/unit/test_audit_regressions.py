"""Defects an audit found. Each was silent — nothing failed when it was wrong."""

from __future__ import annotations

import gc
from contextlib import suppress
from dataclasses import dataclass
from uuid import UUID, uuid4

import pytest
from taskiq import InMemoryBroker
from taskiq_aio_pika import AioPikaBroker

from message_bus import (
    Envelope,
    JsonSerializer,
    MessageBusConfig,
    TransportConfig,
    WorkerFactory,
    as_message,
)
from message_bus.bridge.amqp import AmqpTransportFactory, declared_queues
from message_bus.bridge.taskiq.broker import ensure_started
from message_bus.bridge.taskiq.taskiq_sender import TaskiqSender
from message_bus.bridge.taskiq.taskiq_worker import TaskiqWorker
from message_bus.stamp import (
    DEFAULT_STAMP_TYPES,
    BusNameStamp,
    DelayStamp,
    NonSendableStampInterface,
    TransportMessageIdStamp,
)

pytestmark = pytest.mark.anyio

AMQP = "amqp://guest:guest@localhost:5672/"


@as_message(name="test.audit.v1")
@dataclass(frozen=True, slots=True)
class Audited:
    identifier: UUID


def test_a_producers_stamps_survive_a_default_round_trip() -> None:
    """C1. The default serializer wrote every stamp and restored none.

    Both halves of the AMQP transport default-construct one, so an
    un-customised deploy dropped everything a producer attached.
    """
    wire = JsonSerializer()
    sent = Envelope.wrap(Audited(identifier=uuid4())).with_stamps(
        TransportMessageIdStamp("abc"),
        DelayStamp(5000),
        BusNameStamp("orders"),
    )

    received = wire.decode(wire.encode(sent))

    assert sorted(type(s).__name__ for s in received.stamps) == [
        "BusNameStamp",
        "DelayStamp",
        "TransportMessageIdStamp",
    ]


def test_restoring_nothing_is_still_available_but_must_be_asked_for() -> None:
    wire = JsonSerializer(stamp_types=())
    sent = Envelope.wrap(Audited(identifier=uuid4())).with_stamps(DelayStamp(1))

    assert wire.decode(wire.encode(sent)).stamps == ()


def test_no_default_stamp_type_is_one_that_never_leaves_the_process() -> None:
    """A non-sendable stamp is never written, so restoring it is meaningless."""
    local = [t for t in DEFAULT_STAMP_TYPES if issubclass(t, NonSendableStampInterface)]

    assert local == []


def test_both_halves_of_an_amqp_transport_share_one_serializer() -> None:
    """C2. Producer and consumer are separate deploys; the only guarantee
    available is that an un-customised one is symmetric by construction."""
    mine = JsonSerializer(stamp_types=[DelayStamp])
    factory = AmqpTransportFactory(serializer=mine)

    sender = factory.create({"q": TransportConfig(AMQP, queue="jobs")})["q"]

    assert isinstance(sender, TaskiqSender)
    assert sender.serializer is mine


async def test_a_collected_broker_does_not_hand_its_identity_to_the_next_one() -> None:
    """H1. Started state was keyed by id(), which is reused after collection,
    so a fresh broker could inherit "already started" and never open."""
    started: list[InMemoryBroker] = []

    def counting(broker: InMemoryBroker) -> None:
        original = broker.startup

        async def startup() -> None:
            started.append(broker)
            await original()

        broker.startup = startup

    first = InMemoryBroker()
    counting(first)
    await ensure_started(first)
    assert len(started) == 1

    del first
    _ = gc.collect()

    second = InMemoryBroker()
    counting(second)
    await ensure_started(second)

    assert len(started) == 2


async def test_a_worker_gives_the_broker_back_when_it_stops() -> None:
    """H2. The flag lives on a broker the producing side may share, so
    leaving it set told every later publish a worker owned the connection."""
    broker = InMemoryBroker()
    worker = TaskiqWorker(broker)

    # However run() ends — returning, cancelled, or raising — the broker must
    # not be left claimed. An in-memory broker cannot listen, so this takes
    # the raising path, which is the one that would leak the flag.
    with suppress(BaseException):
        await worker.run()

    assert broker.is_worker_process is False


def test_a_worker_opens_one_connection_not_two() -> None:
    """M1. The worker's own bus built every sender in the configuration and
    then never used one, because a received envelope is never routed."""
    built: list[object] = []
    original = AioPikaBroker.__init__

    def counted(self: AioPikaBroker, *args: str, **kwargs: object) -> None:
        built.append(self)
        original(self, *args, **kwargs)  # pyright: ignore[reportArgumentType]

    # Counting constructions is the only way to see the duplicate; patching a
    # dunder on a third-party class has no typed form.
    AioPikaBroker.__init__ = counted  # pyright: ignore[reportAttributeAccessIssue]
    try:
        config = MessageBusConfig(
            transports={
                "high": TransportConfig(AMQP, queue="jobs_high"),
                "low": TransportConfig(AMQP, queue="jobs_low"),
            },
        )
        _ = WorkerFactory(config).worker(["high"])
    finally:
        AioPikaBroker.__init__ = original

    assert len(built) == 1


def test_a_worker_still_declares_only_the_queues_it_was_given() -> None:
    config = MessageBusConfig(
        transports={
            "high": TransportConfig(AMQP, queue="jobs_high"),
            "low": TransportConfig(AMQP, queue="jobs_low"),
        },
    )

    worker = WorkerFactory(config).worker(["low"])

    assert isinstance(worker, TaskiqWorker)
    broker = worker.broker
    assert isinstance(broker, AioPikaBroker)
    assert list(declared_queues(broker)) == ["jobs_low"]
