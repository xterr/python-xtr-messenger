"""The import layering the bridge tree encodes, enforced.

``bridge/taskiq`` must work with any taskiq broker, so it cannot name one; the
RabbitMQ transport — the xtr-amqp-messenger package — builds on it, never the
reverse; and no module of this package names RabbitMQ's driver. These are read
from the source rather than trusted.
"""

from __future__ import annotations

import pathlib

from taskiq import InMemoryBroker

from tests.support.serialization import json_serializer
from xtr_messenger.bridge.taskiq import StartedBrokers, TaskiqSender, TaskiqWorker

_SOURCE = pathlib.Path(__file__).resolve().parents[3] / "src" / "xtr_messenger"
_BRIDGE = _SOURCE / "bridge"


def _sources(package: str) -> list[pathlib.Path]:
    return sorted(p for p in (_BRIDGE / package).glob("*.py") if p.name != "__init__.py")


def test_the_generic_layer_names_no_broker_driver() -> None:
    """The moment ``bridge/taskiq`` imports aio_pika, the taskiq bridge can no
    longer be installed without aio-pika and a second broker cannot reuse it."""
    offenders = [p.name for p in _sources("taskiq") if "aio_pika" in p.read_text()]

    assert offenders == []


def test_the_dependency_runs_one_way_only() -> None:
    """AMQP builds on taskiq. The reverse would make the split meaningless."""
    offenders = [
        p.name
        for p in _sources("taskiq")
        if "bridge.amqp" in p.read_text() or "xtr_amqp_messenger" in p.read_text()
    ]

    assert offenders == []


def test_no_module_of_the_message_bus_names_the_rabbitmq_driver() -> None:
    """RabbitMQ is the xtr-amqp-messenger package; what is left here only points at it."""
    offenders = [
        str(p.relative_to(_SOURCE))
        for p in sorted(_SOURCE.rglob("*.py"))
        if "aio_pika" in p.read_text()
    ]

    assert offenders == []


def test_publishing_works_on_a_broker_that_is_not_amqp() -> None:
    """The point of the split: this is a broker-agnostic part, not a RabbitMQ one."""
    assert TaskiqSender(InMemoryBroker(), StartedBrokers(), json_serializer()) is not None


def test_consuming_works_on_a_broker_that_is_not_amqp() -> None:
    """The point of the split: this is a broker-agnostic part, not a RabbitMQ one."""
    assert TaskiqWorker(InMemoryBroker()) is not None
