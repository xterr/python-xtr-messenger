"""Running taskiq's own worker behind the library's worker contract."""

from __future__ import annotations

import asyncio
from contextlib import suppress
from typing import TYPE_CHECKING, final
from uuid import uuid4

import pytest
from taskiq import AsyncBroker, InMemoryBroker, TaskiqMessage
from typing_extensions import override

from message_bus import WorkerInterface
from message_bus.bridge.taskiq.broker import ensure_started, forget_started
from message_bus.bridge.taskiq.taskiq_worker import TaskiqWorker

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from taskiq import BrokerMessage

pytestmark = pytest.mark.anyio

_TIMEOUT = 5.0
_CONSUME = "test.unit.worker.consume.v1"


@final
class CountingBroker(InMemoryBroker):
    """Counts startups by overriding ``startup`` rather than patching it."""

    def __init__(self) -> None:
        super().__init__()
        self.startups = 0

    @override
    async def startup(self) -> None:
        self.startups += 1
        await super().startup()


@final
class ScriptedBroker(AsyncBroker):
    """A broker whose ``listen`` yields a scripted backlog, then parks.

    Parking after the backlog is what lets a test prove the worker keeps
    consuming until it is stopped, rather than finishing on its own.
    """

    def __init__(self, task_names: tuple[str, ...] = ()) -> None:
        super().__init__()
        self._task_names = task_names
        self._parked = asyncio.Event()
        self.startups = 0
        self.started = asyncio.Event()
        self.shutdowns: list[bool] = []

    @override
    async def startup(self) -> None:
        self.startups += 1
        await super().startup()
        self.started.set()

    @override
    async def shutdown(self) -> None:
        """Record whether it was still flagged as a worker when shut down."""
        self.shutdowns.append(self.is_worker_process)
        await super().shutdown()

    @override
    async def kick(self, message: BrokerMessage) -> None:
        del message

    @override
    async def listen(self) -> AsyncGenerator[bytes, None]:
        for name in self._task_names:
            message = TaskiqMessage(
                task_id=uuid4().hex,
                task_name=name,
                labels={},
                args=[],
                kwargs={},
            )
            yield self.formatter.dumps(message).message
        _ = await self._parked.wait()


def test_it_is_a_worker_interface() -> None:
    """``stop`` is now part of the contract, so a TaskiqWorker satisfies it."""
    assert isinstance(TaskiqWorker(InMemoryBroker()), WorkerInterface)


def test_stopping_a_worker_that_never_ran_is_harmless() -> None:
    TaskiqWorker(InMemoryBroker()).stop()


def test_it_exposes_the_broker_it_wraps() -> None:
    broker = InMemoryBroker()

    assert TaskiqWorker(broker).broker is broker


async def test_it_restores_the_worker_flag_on_exit() -> None:
    """H2. The flag lives on a broker the producing side may share, so
    leaving it set told every later publish a worker owned the connection.

    An in-memory broker cannot listen, so ``run`` takes the raising path —
    the one that would leak the flag if the restore were missing.
    """
    broker = InMemoryBroker()

    with suppress(BaseException):
        await TaskiqWorker(broker).run()

    assert broker.is_worker_process is False


async def test_a_publish_inside_the_worker_does_not_restart_the_broker() -> None:
    """Restarting re-fires startup events, duplicating whatever they set up —
    invisible until something registered twice fires twice."""
    broker = CountingBroker()
    await ensure_started(broker)
    assert broker.startups == 1

    _ = TaskiqWorker(broker)
    broker.is_worker_process = True
    forget_started(broker)

    await ensure_started(broker)

    assert broker.startups == 1
    forget_started(broker)


async def test_run_consumes_until_stop() -> None:
    calls: list[str] = []
    both = asyncio.Event()
    broker = ScriptedBroker((_CONSUME, _CONSUME))

    async def record() -> None:
        calls.append("x")
        if len(calls) == 2:
            both.set()

    _ = broker.register_task(record, task_name=_CONSUME)
    worker = TaskiqWorker(broker)
    run = asyncio.create_task(worker.run())

    _ = await asyncio.wait_for(both.wait(), timeout=_TIMEOUT)
    assert not run.done()

    worker.stop()
    await asyncio.wait_for(run, timeout=_TIMEOUT)

    assert calls == ["x", "x"]
    assert broker.is_worker_process is False


async def test_a_stopped_worker_shuts_its_broker_down_as_a_worker() -> None:
    """taskiq's receiver starts the broker and leaves closing it to taskiq's
    CLI, which this replaces — without it the connection outlived the worker.
    Shut down while still flagged, so the worker shutdown events fire."""
    broker = ScriptedBroker()
    worker = TaskiqWorker(broker)
    run = asyncio.create_task(worker.run())
    _ = await asyncio.wait_for(broker.started.wait(), timeout=_TIMEOUT)

    worker.stop()
    await asyncio.wait_for(run, timeout=_TIMEOUT)

    assert broker.shutdowns == [True]


async def test_a_producer_sharing_the_broker_opens_it_again_after_the_worker_stops() -> None:
    broker = ScriptedBroker()
    await ensure_started(broker)
    broker.started.clear()
    worker = TaskiqWorker(broker)
    run = asyncio.create_task(worker.run())
    _ = await asyncio.wait_for(broker.started.wait(), timeout=_TIMEOUT)
    worker.stop()
    await asyncio.wait_for(run, timeout=_TIMEOUT)

    await ensure_started(broker)

    assert broker.startups == 3
    forget_started(broker)
