"""Handling a message on a worker raised."""

from __future__ import annotations

from xtr_messenger import Envelope
from xtr_messenger.event import WorkerMessageFailedEvent


def test_it_carries_the_error() -> None:
    error = ValueError("boom")

    event = WorkerMessageFailedEvent(Envelope("m"), "t", error)

    assert event.error is error
    assert event.will_retry is False


def test_set_for_retry_records_that_the_message_comes_back() -> None:
    event = WorkerMessageFailedEvent(Envelope("m"), "t", ValueError("boom"))

    event.set_for_retry()

    assert event.will_retry is True


def test_a_transport_that_retries_can_say_so_up_front() -> None:
    event = WorkerMessageFailedEvent(Envelope("m"), "t", ValueError("boom"), will_retry=True)

    assert event.will_retry is True
