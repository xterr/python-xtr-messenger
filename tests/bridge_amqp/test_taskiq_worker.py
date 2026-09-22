from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from taskiq import InMemoryBroker

from message_bus import MessageBusConfig, TransportConfig, WorkerFactory, WorkerInterface
from message_bus.bridge.taskiq.broker import ensure_started, forget_started
from message_bus.bridge.taskiq.taskiq_worker import TaskiqWorker

if TYPE_CHECKING:
    from collections.abc import Callable, Coroutine

pytestmark = pytest.mark.anyio

AMQP = "amqp://guest:guest@localhost:5672/"


def counting_startup(broker: InMemoryBroker) -> Callable[[], int]:
    started = 0
    original = broker.startup

    async def startup() -> None:
        nonlocal started
        started += 1
        await original()

    replacement: Callable[[], Coroutine[None, None, None]] = startup
    broker.startup = replacement
    return lambda: started


def test_the_amqp_worker_is_only_a_worker_interface_to_its_caller() -> None:
    config = MessageBusConfig(transports={"high": TransportConfig(AMQP, queue="jobs_high")})

    worker = WorkerFactory(config).worker(["high"])

    assert isinstance(worker, WorkerInterface)
    assert isinstance(worker, TaskiqWorker)


async def test_a_publish_inside_the_worker_process_does_not_restart_the_broker() -> None:
    """Restarting re-fires startup events, duplicating whatever they set up.

    Invisible until something registered twice fires twice, so it is pinned
    here rather than left to be noticed in production.
    """
    broker = InMemoryBroker()
    started = counting_startup(broker)
    await ensure_started(broker)
    assert started() == 1

    _ = TaskiqWorker(broker)
    broker.is_worker_process = True
    forget_started(broker)

    await ensure_started(broker)

    assert started() == 1


def test_stopping_a_worker_that_never_ran_is_harmless() -> None:
    TaskiqWorker(InMemoryBroker()).stop()


def test_the_worker_exposes_the_broker_it_wraps_for_inspection() -> None:
    broker = InMemoryBroker()

    assert TaskiqWorker(broker).broker is broker
