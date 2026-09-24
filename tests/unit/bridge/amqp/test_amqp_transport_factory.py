"""Building RabbitMQ transports, and scoping a worker to some of them.

A producer needs one broker knowing every queue it publishes to; a worker
needs a broker consuming only its own. Both come from the same factory, using
real ``AioPikaBroker`` objects — construction opens no connection.
"""

from __future__ import annotations

import pytest
from taskiq import SmartRetryMiddleware
from taskiq_aio_pika import AioPikaBroker

from message_bus import (
    Dsn,
    JsonSerializer,
    MixedDsnError,
    TransportConfig,
    UnknownTransportOptionError,
    WorkerInterface,
    WorkerProvidingInterface,
    name_of,
)
from message_bus.bridge.amqp import AmqpTransportFactory, declared_queues
from message_bus.bridge.taskiq.taskiq_sender import TaskiqSender
from message_bus.bridge.taskiq.taskiq_worker import TaskiqWorker
from message_bus.message_registry import declared_names
from tests.support.fakes import RecordingBus
from tests.support.messages import IngestDocument

_HOST = "amqp://guest:guest@localhost:5672/"
_OTHER = "amqp://guest:guest@other:5672/"


def _group() -> dict[str, TransportConfig]:
    return {
        "high": TransportConfig(_HOST, queue="jobs_high"),
        "low": TransportConfig(_HOST, queue="jobs_low"),
    }


def _sender(built: object) -> TaskiqSender:
    assert isinstance(built, TaskiqSender)
    return built


def _broker_of(sender: object) -> AioPikaBroker:
    broker = _sender(sender).broker
    assert isinstance(broker, AioPikaBroker)
    return broker


def _worker_broker(worker: object) -> AioPikaBroker:
    assert isinstance(worker, TaskiqWorker)
    broker = worker.broker
    assert isinstance(broker, AioPikaBroker)
    return broker


def test_construction_opens_no_connection() -> None:
    senders = AmqpTransportFactory().create(_group())

    assert _broker_of(senders["high"]).write_channel is None


def test_the_producer_broker_declares_every_queue_it_publishes_to() -> None:
    senders = AmqpTransportFactory().create(_group())

    assert declared_queues(_broker_of(senders["high"])) == ("jobs_high", "jobs_low")


def test_a_sender_carries_the_queue_its_transport_named() -> None:
    sender = _sender(AmqpTransportFactory().create(_group())["low"])

    assert sender.queue == "jobs_low"


def test_transports_sharing_a_connection_share_one_broker() -> None:
    senders = AmqpTransportFactory().create(_group())

    assert _broker_of(senders["high"]) is _broker_of(senders["low"])


def test_a_queue_in_the_dsn_still_groups_onto_one_broker() -> None:
    group = {
        "high": TransportConfig(_HOST, queue="jobs_high"),
        "low": TransportConfig(f"{_HOST}?queue=jobs_low"),
    }

    senders = AmqpTransportFactory().create(group)

    assert _broker_of(senders["high"]) is _broker_of(senders["low"])


def test_a_worker_declares_only_the_queues_it_was_given() -> None:
    worker = AmqpTransportFactory().worker({"high": _group()["high"]}, RecordingBus())

    assert declared_queues(_worker_broker(worker)) == ("jobs_high",)


def test_a_worker_can_serve_several_queues_at_once() -> None:
    worker = AmqpTransportFactory().worker(_group(), RecordingBus())

    assert declared_queues(_worker_broker(worker)) == ("jobs_high", "jobs_low")


def test_a_worker_spanning_two_connections_fails_loudly() -> None:
    group = {
        "here": TransportConfig(_HOST, queue="jobs"),
        "there": TransportConfig(_OTHER, queue="jobs"),
    }

    with pytest.raises(MixedDsnError) as excinfo:
        _ = AmqpTransportFactory().worker(group, RecordingBus())

    assert len(excinfo.value.dsns) == 2


def test_the_worker_binds_a_task_for_every_declared_message() -> None:
    """The consumer half registers a task per declared name, dispatching into
    the bus it was given — so ``run`` needs no further wiring."""
    worker = AmqpTransportFactory().worker({"high": _group()["high"]}, RecordingBus())

    tasks = _worker_broker(worker).get_all_tasks()
    assert all(name in tasks for name in declared_names())
    assert name_of(IngestDocument) in tasks


def test_the_worker_is_a_taskiq_worker() -> None:
    worker = AmqpTransportFactory().worker({"high": _group()["high"]}, RecordingBus())

    assert isinstance(worker, WorkerInterface)
    assert isinstance(worker, TaskiqWorker)


def test_both_halves_share_one_serializer() -> None:
    """Producer and consumer are separate deploys; the only guarantee
    available is that an un-customised one is symmetric by construction."""
    mine = JsonSerializer()
    factory = AmqpTransportFactory(serializer=mine)

    sender = _sender(factory.create({"q": TransportConfig(_HOST, queue="jobs")})["q"])

    assert sender.serializer is mine


def test_it_is_a_worker_providing_interface() -> None:
    assert isinstance(AmqpTransportFactory(), WorkerProvidingInterface)


def test_it_supports_the_amqp_schemes() -> None:
    factory = AmqpTransportFactory()

    assert factory.supports(Dsn.parse("amqp://host/"))
    assert factory.supports(Dsn.parse("amqps://host/"))
    assert not factory.supports(Dsn.parse("sync://"))


def test_options_override_the_dsn() -> None:
    """A DSN lives in an environment variable; options let code override it."""
    group = {
        "q": TransportConfig(f"{_HOST}?queue=jobs&max_attempts=3", options={"max_attempts": "9"})
    }

    broker = _broker_of(AmqpTransportFactory().create(group)["q"])

    retry = next(m for m in broker.middlewares if isinstance(m, SmartRetryMiddleware))
    assert retry.default_retry_count == 9


def test_a_misspelled_option_is_refused() -> None:
    """The whole bug: an unread setting leaves the transport on defaults
    nobody chose."""
    group = {"q": TransportConfig(f"{_HOST}?queue=jobs&max_attempt=10")}

    with pytest.raises(UnknownTransportOptionError, match="max_attempt"):
        _ = AmqpTransportFactory().create(group)


def test_an_option_meant_for_another_scheme_is_refused() -> None:
    group = {"q": TransportConfig(f"{_HOST}?queue=jobs&serialize=true")}

    with pytest.raises(UnknownTransportOptionError, match="serialize"):
        _ = AmqpTransportFactory().create(group)
