"""A worker stopped consuming."""

from __future__ import annotations

from tests.support.fakes import RecordingBus, StubReceiver
from xtr_messenger import Worker
from xtr_messenger.event import WorkerStoppedEvent


def test_it_names_the_worker() -> None:
    worker = Worker(RecordingBus(), StubReceiver())

    assert WorkerStoppedEvent(worker).worker is worker
