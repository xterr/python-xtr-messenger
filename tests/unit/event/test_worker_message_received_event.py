"""A message was collected and is about to be handled."""

from __future__ import annotations

from xtr_messenger import Envelope
from xtr_messenger.event import WorkerMessageReceivedEvent


def test_a_received_message_is_handled_unless_a_listener_says_otherwise() -> None:
    assert WorkerMessageReceivedEvent(Envelope("m"), "t").should_handle() is True


def test_should_handle_sets_the_decision_when_given_one() -> None:
    event = WorkerMessageReceivedEvent(Envelope("m"), "t")

    assert event.should_handle(value=False) is False
    assert event.should_handle() is False
