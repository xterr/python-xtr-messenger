"""End-to-end: with the event dispatcher bundle, a kernel's workers announce themselves."""

from __future__ import annotations

import pytest
from xtr_dependency_injection import Kernel

from tests.fixtures.app_worker_events.messages import Ping
from tests.fixtures.app_worker_events.subscribers import WorkerJournal
from xtr_messenger import MessageBusInterface, WorkerFactory

pytestmark = pytest.mark.anyio


async def test_a_subscriber_hears_the_worker_and_its_message() -> None:
    booted = await Kernel("tests.fixtures.app_worker_events", env="test").boot()
    try:
        container = booted.container
        bus = await container.get(MessageBusInterface)
        _ = await bus.dispatch(Ping(1))

        workers = await container.get(WorkerFactory)
        await workers.worker(["jobs"]).run()

        journal = await container.get(WorkerJournal)
        assert journal.entries == [
            "WorkerStartedEvent",
            "WorkerMessageReceivedEvent",
            "WorkerMessageHandledEvent",
            "WorkerStoppedEvent",
        ]
    finally:
        await booted.shutdown()
