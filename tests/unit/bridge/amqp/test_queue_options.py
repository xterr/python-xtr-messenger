"""How the queues a transport reads are declared."""

from __future__ import annotations

import pytest
from taskiq_aio_pika.queue import QueueType

from message_bus import InvalidTransportOptionError
from message_bus.bridge.amqp.queue_options import QueueOptions


def test_the_defaults() -> None:
    options = QueueOptions()

    assert options.type == QueueType.QUORUM.value
    assert options.durable is True
    assert options.auto_delete is False
    assert options.exclusive is False
    assert options.max_priority is None
    assert options.routing_key is None


def test_from_settings_reads_every_field() -> None:
    options = QueueOptions.from_settings(
        {
            "queue_type": "classic",
            "queue_durable": "false",
            "queue_auto_delete": "true",
            "queue_exclusive": "true",
            "queue_max_priority": "5",
            "routing_key": "jobs.key",
        },
    )

    assert options.type == "classic"
    assert options.durable is False
    assert options.auto_delete is True
    assert options.exclusive is True
    assert options.max_priority == 5
    assert options.routing_key == "jobs.key"


def test_settings_override_the_given_defaults() -> None:
    built = QueueOptions.from_settings(
        {"queue_type": "stream"},
        QueueOptions(type="classic", routing_key="kept"),
    )

    assert built.type == "stream"
    assert built.routing_key == "kept"


def test_an_unknown_queue_type_names_the_allowed_values() -> None:
    with pytest.raises(InvalidTransportOptionError) as excinfo:
        _ = QueueOptions.from_settings({"queue_type": "nonsense"})

    assert excinfo.value.option == "queue_type"
    assert "quorum" in excinfo.value.expected


def test_declared_builds_the_queue() -> None:
    queue = QueueOptions(
        type="classic",
        durable=False,
        auto_delete=True,
        exclusive=True,
        max_priority=5,
        routing_key="jobs.key",
    ).declared("jobs", declare=True)

    assert queue.name == "jobs"
    assert queue.type == QueueType.CLASSIC
    assert queue.durable is False
    assert queue.auto_delete is True
    assert queue.exclusive is True
    assert queue.max_priority == 5
    assert queue.routing_key == "jobs.key"
    assert queue.declare is True


def test_declaration_can_be_turned_off() -> None:
    assert QueueOptions().declared("jobs", declare=False).declare is False


def test_a_non_numeric_max_priority_is_refused() -> None:
    with pytest.raises(InvalidTransportOptionError, match="queue_max_priority"):
        _ = QueueOptions.from_settings({"queue_max_priority": "high"})
