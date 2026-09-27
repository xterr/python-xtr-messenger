from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, cast, final

import pytest
from typing_extensions import override

from tests.support.fakes import StubReceiver
from xtr_messenger import AckReceiptStamp, Envelope, ErrorDetailsStamp, ReceivedStamp
from xtr_messenger.transport.receiver.chained_receiver import ChainedReceiver
from xtr_messenger.transport.receiver.receiver_interface import ReceiverInterface

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, AsyncIterator

pytestmark = pytest.mark.anyio


async def drain(receiver: ChainedReceiver) -> list[Envelope]:
    return [envelope async for envelope in receiver.get()]


async def test_it_drains_every_receiver_keeping_each_ones_order() -> None:
    first = StubReceiver([Envelope("a"), Envelope("b")])
    second = StubReceiver([Envelope("c")])
    chained = ChainedReceiver([first, second])

    collected = [str(envelope.message) for envelope in await drain(chained)]

    assert sorted(collected) == ["a", "b", "c"]
    assert collected.index("a") < collected.index("b")


@final
class EndlessReceiver(ReceiverInterface):
    """A subscription that never runs dry, delivering a message whenever asked."""

    @override
    async def get(self) -> AsyncIterator[Envelope]:
        while True:
            await asyncio.sleep(0)
            yield Envelope("endless")

    @override
    async def ack(self, envelope: Envelope) -> None:
        del envelope

    @override
    async def reject(self, envelope: Envelope) -> None:
        del envelope


async def test_a_receiver_that_never_runs_dry_does_not_starve_the_others() -> None:
    chained = ChainedReceiver([EndlessReceiver(), StubReceiver([Envelope("finite")])])
    collected: list[object] = []

    stream = chained.get()
    try:
        async for envelope in stream:
            collected.append(envelope.message)
            if "finite" in collected or len(collected) > 50:
                break
    finally:
        await cast("AsyncGenerator[Envelope]", stream).aclose()

    assert "finite" in collected


async def test_each_collected_envelope_carries_its_own_ticket() -> None:
    origin = StubReceiver([Envelope("a"), Envelope("b")])
    chained = ChainedReceiver([origin])

    collected = await drain(chained)

    assert collected[0].last(AckReceiptStamp) == AckReceiptStamp(1)
    assert collected[1].last(AckReceiptStamp) == AckReceiptStamp(2)


async def test_ack_settles_on_the_origin_with_the_envelope_it_yielded() -> None:
    """The origin gets the envelope as it handed it over, not the ticketed one."""
    yielded = Envelope("a")
    origin = StubReceiver([yielded])
    chained = ChainedReceiver([origin])
    collected = await drain(chained)

    await chained.ack(collected[0])

    assert origin.acked == [yielded]


async def test_ack_routes_to_the_receiver_the_message_came_from() -> None:
    first = StubReceiver([Envelope("a")])
    second = StubReceiver([Envelope("b")])
    chained = ChainedReceiver([first, second])
    collected = await drain(chained)

    await chained.ack(collected[1])

    assert first.acked == []
    assert [envelope.message for envelope in second.acked] == ["b"]


async def test_reject_forwards_the_handed_over_envelope_with_the_origin_receipt() -> None:
    """The consumer's own stamps survive, with the origin's receipt restored as
    the one that counts."""
    yielded = Envelope("a").with_stamps(AckReceiptStamp(99))
    origin = StubReceiver([yielded])
    chained = ChainedReceiver([origin])
    collected = await drain(chained)
    marked = collected[0].with_stamps(ErrorDetailsStamp("RuntimeError", "boom"))

    await chained.reject(marked)

    rejected = origin.rejected[0]
    assert rejected.last(ErrorDetailsStamp) == ErrorDetailsStamp("RuntimeError", "boom")
    assert rejected.last(AckReceiptStamp) == AckReceiptStamp(99)


async def test_reject_leaves_an_untagged_origin_envelope_as_handed_over() -> None:
    """When the origin minted no receipt of its own, the handed-over envelope
    passes through unchanged."""
    origin = StubReceiver([Envelope("a")])
    chained = ChainedReceiver([origin])
    collected = await drain(chained)
    marked = collected[0].with_stamps(ErrorDetailsStamp("RuntimeError", "boom"))

    await chained.reject(marked)

    assert origin.rejected == [marked]


async def test_settling_out_of_order_works() -> None:
    """Correlation is by ticket, not by order, so a caller may settle whenever."""
    origin = StubReceiver([Envelope("a"), Envelope("b")])
    chained = ChainedReceiver([origin])
    collected = await drain(chained)

    await chained.ack(collected[1])
    await chained.ack(collected[0])

    assert [envelope.message for envelope in origin.acked] == ["b", "a"]


async def test_an_untagged_envelope_is_ignored() -> None:
    origin = StubReceiver([Envelope("a")])
    chained = ChainedReceiver([origin])
    _ = await drain(chained)

    await chained.ack(Envelope("never collected"))

    assert origin.acked == []


async def test_a_ticket_for_an_unknown_message_is_ignored() -> None:
    origin = StubReceiver([Envelope("a")])
    chained = ChainedReceiver([origin])
    _ = await drain(chained)

    await chained.reject(Envelope("orphan").with_stamps(AckReceiptStamp(999)))

    assert origin.rejected == []


async def test_settling_the_same_message_twice_is_a_no_op() -> None:
    origin = StubReceiver([Envelope("a")])
    chained = ChainedReceiver([origin])
    collected = await drain(chained)

    await chained.ack(collected[0])
    await chained.ack(collected[0])

    assert len(origin.acked) == 1


async def test_named_receivers_stamp_each_message_with_its_transport() -> None:
    chained = ChainedReceiver(
        [StubReceiver([Envelope("a")]), StubReceiver([Envelope("b")])], ["first", "second"]
    )

    collected = [envelope async for envelope in chained.get()]

    assert [e.last(ReceivedStamp) for e in collected] == [
        ReceivedStamp("first"),
        ReceivedStamp("second"),
    ]


def test_names_must_match_the_receivers_one_for_one() -> None:
    with pytest.raises(ValueError, match="one name per receiver"):
        _ = ChainedReceiver([StubReceiver()], ["a", "b"])
