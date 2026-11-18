from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, final

import anyio
import anyio.lowlevel
import pytest
from xtr_clock import MockClock
from xtr_http_kernel.data_collector import RequestTrace, clone_var

from xtr_messenger import (
    DelayStamp,
    Envelope,
    HandledStamp,
    HandlersFailedError,
    SentStamp,
)
from xtr_messenger.data_collector import DispatchedMessage, TraceableMessageBus
from xtr_messenger.data_collector._masking import MASK

if TYPE_CHECKING:
    from xtr_messenger import MessageBusInterface, StampInterface

pytestmark = pytest.mark.anyio


@dataclass(frozen=True, slots=True)
class _SignUp:
    email: str
    token: str


@final
class _StampingBus:
    """Takes ``takes`` seconds on ``clock``, then hands the envelope back with ``added``."""

    def __init__(self, clock: MockClock, added: tuple[StampInterface, ...], takes: float) -> None:
        self._clock = clock
        self._added = added
        self._takes = takes

    async def dispatch(self, message: object, *stamps: StampInterface) -> Envelope:
        self._clock.sleep(self._takes)
        return Envelope.wrap(message, stamps).with_stamps(*self._added)


@final
class _FailingBus:
    def __init__(self, error: Exception) -> None:
        self._error = error

    async def dispatch(self, message: object, *stamps: StampInterface) -> Envelope:
        del message, stamps
        raise self._error


@final
class _ForwardingBus:
    """Dispatches a follow-up message through ``outer`` while handling the first."""

    def __init__(self) -> None:
        self.outer: MessageBusInterface | None = None

    async def dispatch(self, message: object, *stamps: StampInterface) -> Envelope:
        if message == "first" and self.outer is not None:
            _ = await self.outer.dispatch("second")
        return Envelope.wrap(message, stamps)


def _traced(
    bus: MessageBusInterface, clock: MockClock | None = None
) -> tuple[TraceableMessageBus, RequestTrace[DispatchedMessage]]:
    trace: RequestTrace[DispatchedMessage] = RequestTrace("test_dispatches")
    return TraceableMessageBus(bus, trace, clock), trace


async def test_a_dispatch_outside_a_profiled_request_is_forwarded_and_not_recorded() -> None:
    clock = MockClock()
    bus, trace = _traced(_StampingBus(clock, (), 0), clock)

    envelope = await bus.dispatch("hello")

    assert envelope.message == "hello"
    assert trace.entries() == ()


async def test_a_dispatch_records_the_message_its_stamps_and_what_came_of_it() -> None:
    clock = MockClock()
    added = (HandledStamp("app.welcome", {"id": 7}), SentStamp("app.Sender", "jobs"))
    bus, trace = _traced(_StampingBus(clock, added, 0.25), clock)
    trace.begin()

    _ = await bus.dispatch(_SignUp("ada@example.com", "t-1"), DelayStamp(10))

    [dispatched] = trace.entries()
    assert dispatched.to_data() == {
        "message_class": f"{__name__}._SignUp",
        "message": {
            **clone_var(_SignUp("ada@example.com", "t-1")),
            "attributes": [["email", clone_var("ada@example.com")], ["token", clone_var(MASK)]],
        },
        "stamps_in": ["DelayStamp"],
        "stamps_out": ["DelayStamp", "HandledStamp", "SentStamp"],
        "handled": [{"handler": "app.welcome", "result": clone_var({"id": 7})}],
        "sent": [{"sender": "app.Sender", "alias": "jobs"}],
        "duration_ms": 250.0,
        "error": None,
    }


async def test_an_envelope_dispatched_is_recorded_by_the_message_it_carries() -> None:
    clock = MockClock()
    bus, trace = _traced(_StampingBus(clock, (), 0), clock)
    trace.begin()

    _ = await bus.dispatch(Envelope("hello", (DelayStamp(5),)), SentStamp("s", "a"))

    [dispatched] = trace.entries()
    assert (dispatched.message_class, dispatched.stamps_in) == (
        "builtins.str",
        ("DelayStamp", "SentStamp"),
    )


async def test_failed_handlers_are_recorded_and_the_error_goes_on_unchanged() -> None:
    handled = Envelope("hello", (HandledStamp("app.fine", None),))
    error = HandlersFailedError(handled, {"app.broken": ValueError("no stock")})
    bus, trace = _traced(_FailingBus(error))
    trace.begin()

    with pytest.raises(HandlersFailedError) as raised:
        _ = await bus.dispatch("hello")

    [dispatched] = trace.entries()
    assert raised.value is error
    assert dispatched.error == {
        "class": "xtr_messenger.exception.handlers_failed_error.HandlersFailedError",
        "message": str(error),
        "handlers": [
            {"handler": "app.broken", "class": "builtins.ValueError", "message": "no stock"}
        ],
    }
    assert [entry["handler"] for entry in dispatched.handled] == ["app.fine"]
    assert dispatched.stamps_out == ("HandledStamp",)


async def test_any_other_error_is_recorded_without_handlers() -> None:
    bus, trace = _traced(_FailingBus(RuntimeError("broker down")))
    trace.begin()

    with pytest.raises(RuntimeError, match="broker down"):
        _ = await bus.dispatch("hello")

    [dispatched] = trace.entries()
    assert dispatched.error == {
        "class": "builtins.RuntimeError",
        "message": "broker down",
        "handlers": [],
    }
    assert dispatched.duration_ms is not None


async def test_a_message_dispatched_while_handling_another_comes_after_it() -> None:
    inner = _ForwardingBus()
    bus, trace = _traced(inner)
    inner.outer = bus
    trace.begin()

    _ = await bus.dispatch("first")

    assert [entry.message["value"] for entry in trace.entries()] == ["first", "second"]


async def test_concurrent_requests_each_record_only_their_own_dispatches() -> None:
    clock = MockClock()
    bus, trace = _traced(_StampingBus(clock, (), 0), clock)
    seen: dict[str, list[object]] = {}

    async def request(name: str) -> None:
        trace.begin()
        _ = await bus.dispatch(name)
        await anyio.lowlevel.checkpoint()
        _ = await bus.dispatch(name)
        seen[name] = [entry.message["value"] for entry in trace.entries()]

    async with anyio.create_task_group() as group:
        _ = group.start_soon(request, "a")
        _ = group.start_soon(request, "b")

    assert seen == {"a": ["a", "a"], "b": ["b", "b"]}
