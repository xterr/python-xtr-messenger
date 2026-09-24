"""The library's own receive loop: collect, dispatch, ack or reject."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, final

import pytest
from typing_extensions import override

from message_bus import (
    Envelope,
    ErrorDetailsStamp,
    MessageBusInterface,
    ReceivedStamp,
    ReceiverInterface,
    StampInterface,
    Worker,
    WorkerInterface,
)
from tests.support.fakes import RecordingBus, StubReceiver

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterable

pytestmark = pytest.mark.anyio

_TIMEOUT = 5.0


@final
class SelectiveBus(MessageBusInterface):
    """A bus that raises for one message and dispatches every other."""

    def __init__(self, poison: object) -> None:
        self._poison = poison
        self.dispatched: list[Envelope] = []

    @override
    async def dispatch(self, message: object, *stamps: StampInterface) -> Envelope:
        envelope = Envelope.wrap(message, stamps)
        self.dispatched.append(envelope)
        if envelope.message == self._poison:
            raise ValueError("poison")
        return envelope


@final
class StoppingBus(MessageBusInterface):
    """A bus that stops its worker the moment it dispatches its first message."""

    def __init__(self) -> None:
        self.worker: WorkerInterface | None = None
        self.dispatched: list[Envelope] = []

    @override
    async def dispatch(self, message: object, *stamps: StampInterface) -> Envelope:
        envelope = Envelope.wrap(message, stamps)
        self.dispatched.append(envelope)
        if self.worker is not None:
            self.worker.stop()
        return envelope


@final
class BlockingBus(MessageBusInterface):
    """A bus whose dispatch announces itself, then blocks forever."""

    def __init__(self) -> None:
        self.dispatching = asyncio.Event()
        self._blocked = asyncio.Event()

    @override
    async def dispatch(self, message: object, *stamps: StampInterface) -> Envelope:
        self.dispatching.set()
        _ = await self._blocked.wait()
        return Envelope.wrap(message, stamps)


@final
class RecordingReceiver(ReceiverInterface):
    """A receiver that records which envelopes it actually yielded."""

    def __init__(self, backlog: Iterable[Envelope]) -> None:
        self._backlog = tuple(backlog)
        self.collected: list[Envelope] = []
        self.acked: list[Envelope] = []
        self.rejected: list[Envelope] = []

    @override
    async def get(self) -> AsyncIterator[Envelope]:
        for envelope in self._backlog:
            self.collected.append(envelope)
            yield envelope

    @override
    async def ack(self, envelope: Envelope) -> None:
        self.acked.append(envelope)

    @override
    async def reject(self, envelope: Envelope) -> None:
        self.rejected.append(envelope)


@final
class IdleReceiver(ReceiverInterface):
    """A receiver that announces it is waiting, then never delivers anything."""

    def __init__(self) -> None:
        self.waiting = asyncio.Event()
        self._blocked = asyncio.Event()
        self.acked: list[Envelope] = []
        self.rejected: list[Envelope] = []

    @override
    async def get(self) -> AsyncIterator[Envelope]:
        self.waiting.set()
        _ = await self._blocked.wait()
        empty: tuple[Envelope, ...] = ()
        for envelope in empty:
            yield envelope

    @override
    async def ack(self, envelope: Envelope) -> None:
        self.acked.append(envelope)

    @override
    async def reject(self, envelope: Envelope) -> None:
        self.rejected.append(envelope)


@final
class ExplodingReceiver(ReceiverInterface):
    """A receiver that raises while collecting."""

    @override
    async def get(self) -> AsyncIterator[Envelope]:
        empty: tuple[Envelope, ...] = ()
        for envelope in empty:
            yield envelope
        raise RuntimeError("cannot collect")

    @override
    async def ack(self, envelope: Envelope) -> None:
        del envelope

    @override
    async def reject(self, envelope: Envelope) -> None:
        del envelope


def test_a_worker_is_recognised_by_its_contract_not_its_class() -> None:
    assert isinstance(Worker(RecordingBus(), StubReceiver()), WorkerInterface)


async def test_it_handles_and_acks_each_collected_message() -> None:
    first, second = Envelope("first"), Envelope("second")
    receiver = StubReceiver([first, second])
    bus = RecordingBus()

    await Worker(bus, receiver).run()

    assert bus.dispatched == [first, second]
    assert receiver.acked == [first, second]
    assert receiver.rejected == []


async def test_a_failing_dispatch_rejects_with_the_reason_attached() -> None:
    receiver = StubReceiver([Envelope("a")])
    bus = RecordingBus(failure=ValueError("boom"))

    await Worker(bus, receiver).run()

    assert receiver.acked == []
    assert receiver.rejected[0].last(ErrorDetailsStamp) == ErrorDetailsStamp("ValueError", "boom")


async def test_one_undeliverable_message_does_not_stop_the_worker() -> None:
    """One bad message is rejected and the loop moves on to the next."""
    receiver = StubReceiver([Envelope("good-1"), Envelope("poison"), Envelope("good-2")])
    bus = SelectiveBus("poison")

    await Worker(bus, receiver).run()

    assert [envelope.message for envelope in receiver.acked] == ["good-1", "good-2"]
    assert [envelope.message for envelope in receiver.rejected] == ["poison"]


async def test_the_collected_envelope_is_dispatched_as_it_arrived() -> None:
    """Its stamps — the ReceivedStamp especially — must reach the bus intact,
    because that stamp is what stops the bus routing the message back out."""
    envelope = Envelope("a").with_stamps(ReceivedStamp("in-memory"))
    receiver = StubReceiver([envelope])
    bus = RecordingBus()

    await Worker(bus, receiver).run()

    assert bus.dispatched[0] is envelope
    assert bus.dispatched[0].last(ReceivedStamp) == ReceivedStamp("in-memory")


async def test_stop_returns_at_once_while_the_worker_waits_for_a_message() -> None:
    """A worker parked on an idle receiver has nothing in hand, so stop lets
    ``run`` return without a message ever being settled."""
    receiver = IdleReceiver()
    worker = Worker(RecordingBus(), receiver)
    run = asyncio.ensure_future(worker.run())

    _ = await asyncio.wait_for(receiver.waiting.wait(), timeout=_TIMEOUT)
    worker.stop()
    await asyncio.wait_for(run, timeout=_TIMEOUT)

    assert receiver.acked == []
    assert receiver.rejected == []


async def test_stop_between_messages_leaves_the_next_one_untouched() -> None:
    """A stop requested while a message is in hand settles that message, then
    returns before the next is even collected."""
    receiver = RecordingReceiver([Envelope("first"), Envelope("second")])
    bus = StoppingBus()
    worker = Worker(bus, receiver)
    bus.worker = worker

    await asyncio.wait_for(worker.run(), timeout=_TIMEOUT)

    assert [envelope.message for envelope in receiver.collected] == ["first"]
    assert [envelope.message for envelope in receiver.acked] == ["first"]
    assert receiver.rejected == []


async def test_stop_before_run_and_after_it_returned_are_no_ops() -> None:
    receiver = StubReceiver([Envelope("a")])
    worker = Worker(RecordingBus(), receiver)

    worker.stop()
    await worker.run()
    worker.stop()

    assert [envelope.message for envelope in receiver.acked] == ["a"]


async def test_cancellation_propagates_and_leaves_the_message_unsettled() -> None:
    """Cancellation is not an ``Exception``, so it is not mistaken for a
    message that failed: the envelope is neither acked nor rejected."""
    bus = BlockingBus()
    receiver = StubReceiver([Envelope("a")])
    run = asyncio.ensure_future(Worker(bus, receiver).run())

    _ = await asyncio.wait_for(bus.dispatching.wait(), timeout=_TIMEOUT)
    _ = run.cancel()
    with pytest.raises(asyncio.CancelledError):
        await run

    assert receiver.acked == []
    assert receiver.rejected == []


async def test_a_receiver_that_raises_while_collecting_propagates_out_of_run() -> None:
    with pytest.raises(RuntimeError, match="cannot collect"):
        await Worker(RecordingBus(), ExplodingReceiver()).run()
