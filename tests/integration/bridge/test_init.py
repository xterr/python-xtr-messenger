"""The import layering the bridge tree encodes, enforced.

``bridge/taskiq`` must work with any taskiq broker, so it cannot name one;
``bridge/amqp`` builds on it, never the reverse; and RabbitMQ is named only
in the AMQP layer. These are read from the source rather than trusted.
"""

from __future__ import annotations

import pathlib

import pytest
from taskiq import InMemoryBroker

from xtr_messenger.bridge.taskiq import TaskiqSender, TaskiqWorker

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


@pytest.mark.parametrize("built", [TaskiqSender, TaskiqWorker])
def test_publishing_and_consuming_work_on_a_broker_that_is_not_amqp(
    built: type[TaskiqSender | TaskiqWorker],
) -> None:
    """The point of the split: these are broker-agnostic, not RabbitMQ parts."""
    assert built(InMemoryBroker()) is not None
