"""Under taskiq's worker, run by a command: every message logs in a log unit of its own."""

from __future__ import annotations

import asyncio

import pytest
from xtr_dependency_injection import Injected, Kernel, bind_callable
from xtr_event_dispatcher import EventDispatcher

from tests.fixtures.app_log_units.handlers import UNITS, Logged, log_unit_of_this
from tests.support.taskiq import DeliveringBroker, delivery
from xtr_messenger import Envelope, MessageBusInterface, SerializerInterface, name_of
from xtr_messenger.bridge.taskiq.binding import bind_bus
from xtr_messenger.bridge.taskiq.taskiq_worker import TaskiqWorker
from xtr_messenger.event import (
    AbstractWorkerMessageEvent,
    WorkerMessageFailedEvent,
    WorkerMessageHandledEvent,
)

pytestmark = pytest.mark.anyio

_TIMEOUT = 5.0
_VALUES = ("a", "b", "c", "d", "e")


async def test_each_taskiq_message_logs_in_a_unit_of_its_own() -> None:
    UNITS.clear()
    hosting: list[object | None] = []

    async def consume(
        bus: Injected[MessageBusInterface], serializer: Injected[SerializerInterface]
    ) -> None:
        hosting.append(log_unit_of_this())
        settled: list[AbstractWorkerMessageEvent] = []
        every = asyncio.Event()

        def note(event: AbstractWorkerMessageEvent) -> None:
            settled.append(event)
            if len(settled) == len(_VALUES):
                every.set()

        dispatcher = EventDispatcher()
        dispatcher.add_listener(WorkerMessageHandledEvent, note)
        dispatcher.add_listener(WorkerMessageFailedEvent, note)
        broker = DeliveringBroker(
            [
                delivery(serializer.encode(Envelope(Logged(value))), name_of(Logged))
                for value in _VALUES
            ]
        )
        _ = bind_bus(broker, bus, serializer, event_dispatcher=dispatcher)
        worker = TaskiqWorker(broker)
        run = asyncio.ensure_future(worker.run())
        _ = await asyncio.wait_for(every.wait(), timeout=_TIMEOUT)
        worker.stop()
        await asyncio.wait_for(run, timeout=_TIMEOUT)

    async with await Kernel("tests.fixtures.app_log_units", env="test").boot() as booted:
        _ = await bind_callable(booted.container, consume, per_call_scope=True)()

    assert hosting[0] is not None
    assert None not in UNITS
    assert len({id(unit) for unit in UNITS}) == len(_VALUES)
    assert hosting[0] not in UNITS
