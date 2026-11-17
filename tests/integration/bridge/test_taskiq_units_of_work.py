"""Under taskiq's worker, run by a command: every message at once is a unit of work of its own."""

from __future__ import annotations

import asyncio

import pytest
from xtr_dependency_injection import Injected, Kernel, ServicesResetter, bind_callable
from xtr_event_dispatcher import EventDispatcher

from tests.fixtures.app_overlap.handlers import IN_FLIGHT, SEEN, Overlapping
from tests.fixtures.app_overlap.services import RECORDED, Remembering
from tests.fixtures.app_units.messages import Queued
from tests.fixtures.app_units.services import Journal, Session
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

APP = "tests.fixtures.app_units"
_TIMEOUT = 5.0
_VALUES = ("a", "b", "c", "d", "e")


async def test_messages_handled_at_once_each_hold_their_follow_up_back_in_a_unit_of_its_own() -> (
    None
):
    """The command's unit reaches none of them: five messages in flight, and
    the follow-up each holds back, are ten units — never the command's one."""
    hosting: list[Session] = []

    async def consume(
        session: Injected[Session],
        bus: Injected[MessageBusInterface],
        serializer: Injected[SerializerInterface],
    ) -> None:
        hosting.append(session)
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
                delivery(serializer.encode(Envelope(Queued(value))), name_of(Queued))
                for value in _VALUES
            ]
        )
        _ = bind_bus(broker, bus, serializer, event_dispatcher=dispatcher)
        worker = TaskiqWorker(broker)
        run = asyncio.ensure_future(worker.run())
        _ = await asyncio.wait_for(every.wait(), timeout=_TIMEOUT)
        worker.stop()
        await asyncio.wait_for(run, timeout=_TIMEOUT)

    async with await Kernel(APP, env="test").boot() as booted:
        journal = await booted.container.get(Journal)

        _ = await bind_callable(booted.container, consume, per_call_scope=True)()

    numbers = {session.number for _, session in journal.sessions}
    assert len(journal.sessions) == 2 * len(_VALUES)
    assert len(numbers) == 2 * len(_VALUES)
    assert hosting[0].number not in numbers


_OVERLAPPING = ("a", "b", "boom", "d", "e")


async def test_messages_in_flight_together_keep_their_sessions_and_log_buffers_apart() -> None:
    """Every message waits until all five are in flight, with the resetter running after each:
    each still has a session of its own, closed after it, and only the failing one's held
    records are written."""
    SEEN.clear()
    RECORDED.clear()
    IN_FLIGHT[:] = [asyncio.Barrier(len(_OVERLAPPING))]

    async def consume(
        bus: Injected[MessageBusInterface],
        serializer: Injected[SerializerInterface],
        resetter: Injected[ServicesResetter],
    ) -> None:
        settled: list[AbstractWorkerMessageEvent] = []
        every = asyncio.Event()

        def note(event: AbstractWorkerMessageEvent) -> None:
            settled.append(event)
            if len(settled) == len(_OVERLAPPING):
                every.set()

        dispatcher = EventDispatcher()
        dispatcher.add_listener(WorkerMessageHandledEvent, note)
        dispatcher.add_listener(WorkerMessageFailedEvent, note)
        broker = DeliveringBroker(
            [
                delivery(serializer.encode(Envelope(Overlapping(value))), name_of(Overlapping))
                for value in _OVERLAPPING
            ]
        )
        _ = bind_bus(broker, bus, serializer, event_dispatcher=dispatcher, resetter=resetter)
        worker = TaskiqWorker(broker)
        run = asyncio.ensure_future(worker.run())
        _ = await asyncio.wait_for(every.wait(), timeout=_TIMEOUT)
        worker.stop()
        await asyncio.wait_for(run, timeout=_TIMEOUT)

    async with await Kernel("tests.fixtures.app_overlap", env="test").boot() as booted:
        _ = await bind_callable(booted.container, consume, per_call_scope=True)()
        remembering = await booted.container.get(Remembering)

    sessions = [session for _, session in SEEN]
    assert sorted(value for value, _ in SEEN) == sorted(_OVERLAPPING)
    assert len({session.number for session in sessions}) == len(_OVERLAPPING)
    assert all(session.closed for session in sessions)
    assert RECORDED == ["handling {value} boom", "failed {value} boom"]
    assert remembering.resets >= len(_OVERLAPPING)
