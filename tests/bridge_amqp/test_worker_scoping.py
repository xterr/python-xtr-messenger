from __future__ import annotations

import pytest
from taskiq_aio_pika import AioPikaBroker

from message_bus import (
    MessageBusConfig,
    TransportConfig,
    TransportFactoryInterface,
    Worker,
    WorkerFactory,
    default_factories,
)
from message_bus.bridge.amqp import AmqpTransportFactory, MixedDsnError, declared_queues
from message_bus.bridge.taskiq.taskiq_sender import TaskiqSender
from message_bus.bridge.taskiq.taskiq_worker import TaskiqWorker

AMQP = "amqp://guest:guest@localhost:5672/"
OTHER_AMQP = "amqp://guest:guest@other:5672/"


def a_config() -> MessageBusConfig:
    return MessageBusConfig(
        transports={
            "high": TransportConfig(AMQP, queue="jobs_high"),
            "low": TransportConfig(AMQP, queue="jobs_low"),
        },
    )


def amqp_senders(config: MessageBusConfig) -> object:
    return AmqpTransportFactory().create(config.transports)


def queues_of(broker: object) -> list[str]:
    assert isinstance(broker, AioPikaBroker)
    return list(declared_queues(broker))


def worker_broker(worker: object) -> AioPikaBroker:
    assert isinstance(worker, TaskiqWorker)
    assert isinstance(worker.broker, AioPikaBroker)
    return worker.broker


def broker_of(sender: object) -> AioPikaBroker:
    assert isinstance(sender, TaskiqSender)
    assert isinstance(sender.broker, AioPikaBroker)
    return sender.broker


def test_transports_sharing_a_connection_share_one_broker() -> None:
    senders = AmqpTransportFactory().create(a_config().transports)

    assert broker_of(senders["high"]) is broker_of(senders["low"])


def test_a_queue_in_the_dsn_still_groups_onto_the_same_broker() -> None:
    config = MessageBusConfig(
        transports={
            "high": TransportConfig(AMQP, queue="jobs_high"),
            "low": TransportConfig(f"{AMQP}?queue=jobs_low"),
        },
    )

    senders = AmqpTransportFactory().create(config.transports)

    assert broker_of(senders["high"]) is broker_of(senders["low"])


def test_the_producer_broker_declares_every_queue_it_publishes_to() -> None:
    senders = AmqpTransportFactory().create(a_config().transports)

    assert queues_of(broker_of(senders["high"])) == ["jobs_high", "jobs_low"]


def test_a_worker_listens_only_on_the_queues_it_was_given() -> None:
    assert queues_of(worker_broker(WorkerFactory(a_config()).worker(["high"]))) == ["jobs_high"]


def test_a_worker_for_the_other_queue_is_unaffected() -> None:
    assert queues_of(worker_broker(WorkerFactory(a_config()).worker(["low"]))) == ["jobs_low"]


def test_a_worker_can_serve_several_queues_at_once() -> None:
    assert queues_of(worker_broker(WorkerFactory(a_config()).worker(["high", "low"]))) == [
        "jobs_high",
        "jobs_low",
    ]


def test_a_worker_spanning_two_brokers_fails_loudly() -> None:
    config = MessageBusConfig(
        transports={
            "here": TransportConfig(AMQP, queue="jobs"),
            "there": TransportConfig(OTHER_AMQP, queue="jobs"),
        },
    )

    with pytest.raises(MixedDsnError) as excinfo:
        _ = WorkerFactory(config).worker(["here", "there"])

    assert len(excinfo.value.dsns) == 2


def test_building_a_worker_broker_opens_no_connection() -> None:
    broker = worker_broker(WorkerFactory(a_config()).worker(["high"]))

    assert broker.write_channel is None


@pytest.mark.anyio
async def test_a_sync_worker_runs_and_finishes_without_a_backlog() -> None:
    """sync:// keeps nothing to collect, so its worker completes at once.

    It used to raise instead. Returning a worker that finishes lets one
    entrypoint name a list of transports that happens to include sync://.
    """
    config = MessageBusConfig(transports={"sync": TransportConfig("sync://")})

    worker = WorkerFactory(config).worker(["sync"])
    await worker.run()

    assert isinstance(worker, Worker)


def test_a_sender_carries_the_queue_its_transport_named() -> None:
    sender = AmqpTransportFactory().create(a_config().transports)["low"]

    assert isinstance(sender, TaskiqSender)
    assert sender.queue == "jobs_low"


def test_every_builtin_factory_satisfies_the_protocol() -> None:
    for factory in default_factories():
        assert isinstance(factory, TransportFactoryInterface)
