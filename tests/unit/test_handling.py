from __future__ import annotations

import logging
from typing import final, override

import pytest
from xtr_event_dispatcher import EventDispatcher

from xtr_messenger import (
    DelayedMessageHandlingError,
    Envelope,
    ErrorDetailsStamp,
    MessageBusInterface,
    StampInterface,
)
from xtr_messenger._handling import Failed, Handled, handle
from xtr_messenger.event import WorkerMessageFailedEvent, WorkerMessageHandledEvent

pytestmark = pytest.mark.anyio


@final
class FollowUpFailingBus(MessageBusInterface):
    """A bus whose message succeeds and whose held-back follow-ups then fail."""

    def __init__(self, *follow_ups: tuple[Envelope, Exception]) -> None:
        self._follow_ups = follow_ups

    @override
    async def dispatch(self, message: object, *stamps: StampInterface) -> Envelope:
        handled = Envelope.wrap(message, stamps)
        raise DelayedMessageHandlingError(handled, self._follow_ups)


async def test_a_failing_follow_up_does_not_fail_its_handled_parent() -> None:
    dispatcher = EventDispatcher()
    failures: list[WorkerMessageFailedEvent] = []
    dispatcher.add_listener(WorkerMessageFailedEvent, failures.append)
    handled: list[WorkerMessageHandledEvent] = []
    dispatcher.add_listener(WorkerMessageHandledEvent, handled.append)
    bus = FollowUpFailingBus(
        (Envelope("receipt"), ValueError("no mail")), (Envelope("audit"), KeyError("gone"))
    )

    outcome = await handle(bus, Envelope("order"), "jobs", dispatcher)

    assert isinstance(outcome, Handled)
    assert outcome.envelope.message == "order"
    assert [(event.envelope.message, event.will_retry) for event in failures] == [
        ("receipt", False),
        ("audit", False),
    ]
    assert failures[0].envelope.last(ErrorDetailsStamp) == ErrorDetailsStamp(
        "ValueError", "no mail"
    )
    assert isinstance(failures[1].error, KeyError)
    assert [event.envelope.message for event in handled] == ["order"]


async def test_a_failing_follow_up_without_a_dispatcher_still_leaves_its_parent_handled() -> None:
    bus = FollowUpFailingBus((Envelope("receipt"), ValueError("no mail")))

    outcome = await handle(bus, Envelope("order"), "jobs", None)

    assert isinstance(outcome, Handled)


async def test_a_raising_follow_up_failure_listener_is_logged_and_the_parent_still_handled(
    caplog: pytest.LogCaptureFixture,
) -> None:
    dispatcher = EventDispatcher()

    def broken(event: WorkerMessageFailedEvent) -> None:
        del event
        raise RuntimeError("listener bug")

    dispatcher.add_listener(WorkerMessageFailedEvent, broken)
    bus = FollowUpFailingBus((Envelope("receipt"), ValueError("no mail")))

    with caplog.at_level(logging.ERROR, logger="messenger"):
        outcome = await handle(bus, Envelope("order"), "jobs", dispatcher)

    assert isinstance(outcome, Handled)
    assert "listener bug" in caplog.text


@final
class PassingBus(MessageBusInterface):
    @override
    async def dispatch(self, message: object, *stamps: StampInterface) -> Envelope:
        return Envelope.wrap(message, stamps)


async def test_a_handled_listener_failure_still_acks(caplog: pytest.LogCaptureFixture) -> None:
    dispatcher = EventDispatcher()

    def broken(event: WorkerMessageHandledEvent) -> None:
        del event
        raise RuntimeError("listener bug")

    dispatcher.add_listener(WorkerMessageHandledEvent, broken)

    with caplog.at_level(logging.ERROR, logger="messenger"):
        outcome = await handle(PassingBus(), Envelope("order"), "jobs", dispatcher)

    assert isinstance(outcome, Handled)
    assert outcome.envelope.message == "order"
    assert "listener bug" in caplog.text


@final
class FailingBus(MessageBusInterface):
    @override
    async def dispatch(self, message: object, *stamps: StampInterface) -> Envelope:
        del message, stamps
        raise ValueError("boom")


async def test_a_message_whose_own_handling_fails_is_still_failed() -> None:
    outcome = await handle(FailingBus(), Envelope("order"), "jobs", EventDispatcher())

    assert isinstance(outcome, Failed)
    assert isinstance(outcome.error, ValueError)
