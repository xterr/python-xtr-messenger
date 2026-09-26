"""What every event about one message on a worker carries."""

from __future__ import annotations

from xtr_messenger import DelayStamp, Envelope
from xtr_messenger.event import WorkerMessageHandledEvent


def test_it_reports_the_envelope_and_the_transport_it_came_from() -> None:
    envelope = Envelope("payload")

    event = WorkerMessageHandledEvent(envelope, "orders")

    assert event.envelope is envelope
    assert event.receiver_name == "orders"


def test_added_stamps_land_on_the_envelope_the_worker_carries_on_with() -> None:
    event = WorkerMessageHandledEvent(Envelope("payload"), "orders")

    event.add_stamps(DelayStamp(10))

    assert event.envelope.last(DelayStamp) == DelayStamp(10)
