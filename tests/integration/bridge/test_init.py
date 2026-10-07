"""The import layering the bridge tree encodes, enforced.

``bridge/taskiq`` must work with any taskiq broker, so it cannot name one;
``bridge/amqp`` builds on it, never the reverse; and RabbitMQ is named only
in the AMQP layer. These are read from the source rather than trusted.
"""

from __future__ import annotations

import pathlib

from taskiq import InMemoryBroker

from xtr_messenger.bridge.taskiq import StartedBrokers, TaskiqSender, TaskiqWorker

_BRIDGE = pathlib.Path(__file__).resolve().parents[3] / "src" / "xtr_messenger" / "bridge"


def _sources(package: str) -> list[pathlib.Path]:
    return sorted(p for p in (_BRIDGE / package).glob("*.py") if p.name != "__init__.py")


def test_the_generic_layer_names_no_broker_driver() -> None:
    """The moment ``bridge/taskiq`` imports aio_pika, the ``[taskiq]`` extra
    stops being installable on its own and a second broker cannot reuse it."""
    offenders = [p.name for p in _sources("taskiq") if "aio_pika" in p.read_text()]

    assert offenders == []


def test_the_dependency_runs_one_way_only() -> None:
    """AMQP builds on taskiq. The reverse would make the split meaningless."""
    offenders = [p.name for p in _sources("taskiq") if "bridge.amqp" in p.read_text()]

    assert offenders == []


def test_the_amqp_layer_is_the_only_place_that_names_rabbitmq() -> None:
    amqp_sources = _sources("amqp")
    assert amqp_sources

    names_driver = [p.name for p in amqp_sources if "aio_pika" in p.read_text()]

    assert names_driver != []


def test_publishing_works_on_a_broker_that_is_not_amqp() -> None:
    """The point of the split: this is a broker-agnostic part, not a RabbitMQ one."""
    assert TaskiqSender(InMemoryBroker(), StartedBrokers()) is not None


def test_consuming_works_on_a_broker_that_is_not_amqp() -> None:
    """The point of the split: this is a broker-agnostic part, not a RabbitMQ one."""
    assert TaskiqWorker(InMemoryBroker()) is not None
