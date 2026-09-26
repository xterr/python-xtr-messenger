"""A worker settled a message and is going on."""

from __future__ import annotations

from tests.support.fakes import RecordingBus, StubReceiver
from xtr_messenger import Worker
from xtr_messenger.event import WorkerRunningEvent


def test_it_names_the_worker_and_is_not_idle_by_default() -> None:
    worker = Worker(RecordingBus(), StubReceiver())

    event = WorkerRunningEvent(worker)

    assert event.worker is worker
    assert event.is_idle is False


def test_it_can_report_an_idle_worker() -> None:
    assert WorkerRunningEvent(Worker(RecordingBus(), StubReceiver()), is_idle=True).is_idle
