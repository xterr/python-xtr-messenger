from __future__ import annotations

from dataclasses import dataclass
from typing import final

import pytest

from message_bus import Envelope, MessageBus, MessageBusInterface, StackInterface, StampInterface

pytestmark = pytest.mark.anyio


@dataclass(frozen=True, slots=True)
class Marker(StampInterface):
    value: str


@final
class Recording:
    def __init__(self, name: str, calls: list[str]) -> None:
        self._name = name
        self._calls = calls

    async def handle(self, envelope: Envelope, stack: StackInterface, /) -> Envelope:
        self._calls.append(self._name)
        return await stack.next().handle(envelope, stack)


@final
class ShortCircuit:
    def __init__(self, name: str, calls: list[str]) -> None:
        self._name = name
        self._calls = calls

    async def handle(self, envelope: Envelope, _stack: StackInterface, /) -> Envelope:
        self._calls.append(self._name)
        return envelope


@final
class Capturing:
    def __init__(self) -> None:
        self.seen: list[Envelope] = []

    async def handle(self, envelope: Envelope, _stack: StackInterface, /) -> Envelope:
        self.seen.append(envelope)
        return envelope


@final
class Exploding:
    async def handle(self, _envelope: Envelope, _stack: StackInterface, /) -> Envelope:
        raise RuntimeError(self.__class__.__name__)


async def test_middleware_runs_in_composition_order() -> None:
    calls: list[str] = []
    bus = MessageBus([Recording("first", calls), Recording("second", calls)])

    await bus.dispatch("payload")

    assert calls == ["first", "second"]


async def test_short_circuit_skips_everything_downstream() -> None:
    calls: list[str] = []
    bus = MessageBus(
        [Recording("before", calls), ShortCircuit("stop", calls), Recording("after", calls)]
    )

    await bus.dispatch("payload")

    assert calls == ["before", "stop"]


async def test_an_empty_chain_is_a_no_op_dispatch() -> None:
    result = await MessageBus([]).dispatch("payload")

    assert result.message == "payload"
    assert result.stamps == ()


async def test_delegating_past_the_last_middleware_returns_the_envelope() -> None:
    calls: list[str] = []

    result = await MessageBus([Recording("only", calls)]).dispatch("payload")

    assert calls == ["only"]
    assert result.message == "payload"


async def test_a_raw_message_reaches_the_chain_wrapped_and_stampless() -> None:
    capture = Capturing()

    await MessageBus([capture]).dispatch("payload")

    assert capture.seen[0].message == "payload"
    assert capture.seen[0].stamps == ()


async def test_dispatch_stamps_are_attached_before_the_chain_runs() -> None:
    capture = Capturing()

    await MessageBus([capture]).dispatch("payload", Marker("up-front"))

    assert capture.seen[0].all(Marker) == (Marker("up-front"),)


async def test_an_existing_envelope_is_redispatched_with_its_stamps_intact() -> None:
    envelope = Envelope("payload", stamps=(Marker("earlier"),))
    capture = Capturing()

    await MessageBus([capture]).dispatch(envelope)

    assert capture.seen[0] is envelope


async def test_each_dispatch_gets_a_fresh_cursor() -> None:
    calls: list[str] = []
    bus = MessageBus([Recording("only", calls)])

    await bus.dispatch("one")
    await bus.dispatch("two")

    assert calls == ["only", "only"]


async def test_middleware_exceptions_propagate_untouched() -> None:
    bus = MessageBus([Exploding()])

    with pytest.raises(RuntimeError, match="Exploding"):
        await bus.dispatch("payload")


async def test_mutating_the_middleware_list_afterwards_cannot_change_the_bus() -> None:
    calls: list[str] = []
    middlewares = [Recording("only", calls)]
    bus = MessageBus(middlewares)
    middlewares.append(Recording("sneaky", calls))

    await bus.dispatch("payload")

    assert calls == ["only"]


def test_the_bus_satisfies_the_port_publishers_depend_on() -> None:
    assert isinstance(MessageBus([]), MessageBusInterface)
