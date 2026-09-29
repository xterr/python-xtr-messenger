"""Unit tests for :class:`xtr_messenger.middleware.DispatchAfterCurrentBusMiddleware`."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import final

import pytest
from typing_extensions import override

from xtr_messenger import (
    DelayedMessageHandlingError,
    DispatchAfterCurrentBusMiddleware,
    DispatchAfterCurrentBusStamp,
    Envelope,
    MessageBus,
    MiddlewareInterface,
    StackInterface,
)

pytestmark = pytest.mark.anyio


@dataclass(frozen=True, slots=True)
class Note:
    name: str


Step = Callable[[], Awaitable[None]]


@final
class Handling(MiddlewareInterface):
    """The end of a chain: notes each message it handles, then runs what that message asks."""

    def __init__(self, log: list[str]) -> None:
        self.log = log
        self.steps: dict[str, list[Step]] = {}
        self.failing: set[str] = set()

    @override
    async def handle(self, envelope: Envelope, stack: StackInterface, /) -> Envelope:
        del stack
        message = envelope.message
        assert isinstance(message, Note)
        self.log.append(f"handle {message.name}")
        for step in self.steps.get(message.name, []):
            await step()
        self.log.append(f"done {message.name}")
        if message.name in self.failing:
            raise LookupError(message.name)
        return envelope


def a_bus(handling: Handling) -> MessageBus:
    return MessageBus([DispatchAfterCurrentBusMiddleware(), handling])


def later(bus: MessageBus, name: str) -> Step:
    async def dispatch() -> None:
        _ = await bus.dispatch(Note(name), DispatchAfterCurrentBusStamp())

    return dispatch


def now(bus: MessageBus, name: str) -> Step:
    async def dispatch() -> None:
        _ = await bus.dispatch(Note(name))

    return dispatch


async def test_a_stamped_message_waits_until_the_current_one_was_handled() -> None:
    log: list[str] = []
    handling = Handling(log)
    bus = a_bus(handling)
    handling.steps["order"] = [later(bus, "receipt")]

    _ = await bus.dispatch(Note("order"))

    assert log == ["handle order", "done order", "handle receipt", "done receipt"]


async def test_a_message_dispatched_without_the_stamp_goes_out_at_once() -> None:
    log: list[str] = []
    handling = Handling(log)
    bus = a_bus(handling)
    handling.steps["order"] = [now(bus, "receipt")]

    _ = await bus.dispatch(Note("order"))

    assert log == ["handle order", "handle receipt", "done receipt", "done order"]


async def test_held_messages_are_dropped_when_the_current_one_fails() -> None:
    log: list[str] = []
    handling = Handling(log)
    bus = a_bus(handling)
    handling.steps["order"] = [later(bus, "receipt")]
    handling.failing.add("order")

    with pytest.raises(LookupError):
        _ = await bus.dispatch(Note("order"))

    assert log == ["handle order", "done order"]


async def test_a_stamped_message_with_nothing_handled_goes_out_at_once_without_the_stamp() -> None:
    log: list[str] = []
    bus = a_bus(Handling(log))

    envelope = await bus.dispatch(Note("receipt"), DispatchAfterCurrentBusStamp())

    assert log == ["handle receipt", "done receipt"]
    assert envelope.last(DispatchAfterCurrentBusStamp) is None


async def test_the_held_envelope_is_returned_unhandled_and_without_the_stamp() -> None:
    returned: list[Envelope] = []
    handling = Handling([])
    bus = a_bus(handling)

    async def hold() -> None:
        returned.append(await bus.dispatch(Note("receipt"), DispatchAfterCurrentBusStamp()))

    handling.steps["order"] = [hold]

    _ = await bus.dispatch(Note("order"))

    assert returned[0].message == Note("receipt")
    assert returned[0].last(DispatchAfterCurrentBusStamp) is None


async def test_a_message_dispatched_through_another_bus_waits_for_the_current_one() -> None:
    """A worker and the bus its handlers dispatch through are two chains."""
    log: list[str] = []
    publishing = Handling(log)
    other = a_bus(publishing)
    working = Handling(log)
    worker = a_bus(working)
    working.steps["order"] = [later(other, "receipt")]

    _ = await worker.dispatch(Note("order"))

    assert log == ["handle order", "done order", "handle receipt", "done receipt"]


async def test_held_messages_go_out_in_order_and_may_hold_back_more() -> None:
    log: list[str] = []
    handling = Handling(log)
    bus = a_bus(handling)
    handling.steps["order"] = [later(bus, "receipt"), later(bus, "audit")]
    handling.steps["receipt"] = [later(bus, "archive")]

    _ = await bus.dispatch(Note("order"))

    handled = [entry for entry in log if entry.startswith("handle")]
    assert handled == ["handle order", "handle receipt", "handle audit", "handle archive"]


async def test_every_held_message_is_tried_and_their_failures_raised_together() -> None:
    log: list[str] = []
    handling = Handling(log)
    bus = a_bus(handling)
    handling.steps["order"] = [later(bus, "receipt"), later(bus, "audit"), later(bus, "archive")]
    handling.failing.update({"receipt", "archive"})

    with pytest.raises(DelayedMessageHandlingError) as raised:
        _ = await bus.dispatch(Note("order"))

    assert [str(error) for error in raised.value.errors] == ["receipt", "archive"]
    assert raised.value.envelope.message == Note("order")
    assert isinstance(raised.value.__cause__, LookupError)
    assert "handle audit" in log


async def test_a_stamped_message_dispatched_once_the_current_one_is_done_goes_out_at_once() -> None:
    """What a handler left running past its message is not held by a message long gone."""
    log: list[str] = []
    handling = Handling(log)
    bus = a_bus(handling)
    pending: list[Step] = []

    async def keep() -> None:
        pending.append(later(bus, "receipt"))

    handling.steps["order"] = [keep]

    _ = await bus.dispatch(Note("order"))
    await pending[0]()

    assert log == ["handle order", "done order", "handle receipt", "done receipt"]
