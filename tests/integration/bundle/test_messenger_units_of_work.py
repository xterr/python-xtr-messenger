"""With a kernel, a message is a unit of work, and a chain entry can give middleware arguments."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from xtr_dependency_injection import (
    Injected,
    Kernel,
    bind_callable,
    current_unit_of_work,
    unit_of_work,
)
from xtr_event_dispatcher import EventDispatcher

from tests.fixtures.app_units.messages import Queued, Record
from tests.fixtures.app_units.services import Journal, Session
from tests.support.fakes import RecordingBus, StubReceiver
from xtr_messenger import (
    Envelope,
    MessageBusInterface,
    MiddlewareInterface,
    Worker,
    WorkerFactory,
)
from xtr_messenger.event import WorkerMessageHandledEvent

if TYPE_CHECKING:
    from xtr_service_contracts import ContainerInterface

pytestmark = pytest.mark.anyio

APP = "tests.fixtures.app_units"


async def test_every_handler_of_a_message_shares_one_session_released_with_the_message() -> None:
    async with await Kernel(APP, env="test").boot() as booted:
        bus = await booted.container.get(MessageBusInterface)
        journal = await booted.container.get(Journal)

        _ = await bus.dispatch(Envelope(Record("a")))
        _ = await bus.dispatch(Envelope(Record("b")))

    seen = dict(journal.sessions)
    assert seen["first:a"] is seen["second:a"] is seen["nested:a"]
    assert seen["first:b"] is seen["second:b"] is seen["nested:b"]
    assert seen["first:a"] is not seen["first:b"]
    assert all(session.closed for session in seen.values())


async def test_a_chain_entry_with_arguments_is_a_middleware_of_its_own() -> None:
    async with await Kernel(APP, env="test").boot() as booted:
        bus = await booted.container.get(MessageBusInterface)
        journal = await booted.container.get(Journal)

        _ = await bus.dispatch(Envelope(Record("a")))

        bare = await booted.container.get(MiddlewareInterface, "labelled")
        ordered = await booted.container.get(MiddlewareInterface, "labelled#0")
        assert bare is not ordered

    # The nested dispatch runs the chain again, inside the same unit of work.
    assert journal.middleware[:3] == [("plain", 1), ("first", 1), ("plain", 2)]


async def test_a_message_held_back_in_a_worker_a_command_runs_is_a_unit_of_its_own() -> None:
    """The command's unit is not its worker's messages': one released once the
    message that held it back is done with joins nothing of the command's."""
    hosting: list[Session] = []

    async def consume(session: Injected[Session], workers: Injected[WorkerFactory]) -> None:
        hosting.append(session)
        await workers.worker(["jobs"]).run()

    async with await Kernel(APP, env="test").boot() as booted:
        bus = await booted.container.get(MessageBusInterface)
        journal = await booted.container.get(Journal)
        _ = await bus.dispatch(Envelope(Queued("a")))
        _ = await bus.dispatch(Envelope(Queued("b")))

        _ = await bind_callable(booted.container, consume, per_call_scope=True)()

    seen = dict(journal.sessions)
    sessions = [
        hosting[0],
        seen["queued:a"],
        seen["follow_up:a"],
        seen["queued:b"],
        seen["follow_up:b"],
    ]
    assert len({session.number for session in sessions}) == 5


async def test_a_listener_of_a_worker_s_messages_runs_outside_the_unit_its_worker_runs_in() -> None:
    """Nothing opens a unit around it, so what it resolves is its own call's —
    never the scoped services of whatever runs the worker."""
    units: list[ContainerInterface | None] = []
    dispatcher = EventDispatcher()

    def note(event: WorkerMessageHandledEvent) -> None:
        del event
        units.append(current_unit_of_work())

    dispatcher.add_listener(WorkerMessageHandledEvent, note)
    async with await Kernel(APP, env="test").boot() as booted, unit_of_work(booted.container):
        worker = Worker(RecordingBus(), StubReceiver([Envelope("a")]), event_dispatcher=dispatcher)
        await worker.run()

    assert units == [None]
