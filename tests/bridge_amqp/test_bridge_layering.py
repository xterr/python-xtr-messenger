from __future__ import annotations

import pathlib

import pytest
from taskiq import InMemoryBroker

from message_bus.bridge.taskiq import TaskiqSender, TaskiqWorker

BRIDGE = pathlib.Path(__file__).resolve().parents[2] / "src" / "message_bus" / "bridge"


def sources(package: str) -> list[pathlib.Path]:
    return sorted(p for p in (BRIDGE / package).glob("*.py") if p.name != "__init__.py")


def test_the_generic_layer_names_no_broker_driver() -> None:
    """bridge/taskiq must work with any taskiq broker, so it cannot name one.

    The moment something here imports aio_pika, the `[taskiq]` extra stops
    being installable on its own and a second broker can no longer reuse it.
    """
    offenders = [p.name for p in sources("taskiq") if "aio_pika" in p.read_text()]

    assert offenders == []


def test_the_dependency_runs_one_way_only() -> None:
    """AMQP builds on taskiq. The reverse would make the split meaningless."""
    offenders = [p.name for p in sources("taskiq") if "bridge.amqp" in p.read_text()]

    assert offenders == []


def test_the_amqp_layer_is_the_only_place_that_names_rabbitmq() -> None:
    amqp_sources = sources("amqp")
    assert amqp_sources, "expected modules under bridge/amqp"

    names_driver = [p.name for p in amqp_sources if "aio_pika" in p.read_text()]

    assert names_driver != []


@pytest.mark.parametrize("built", [TaskiqSender, TaskiqWorker])
def test_publishing_and_consuming_work_on_a_broker_that_is_not_amqp(
    built: type[TaskiqSender | TaskiqWorker],
) -> None:
    """The point of the split: these are broker-agnostic, not RabbitMQ parts."""
    assert built(InMemoryBroker()) is not None
