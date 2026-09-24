from __future__ import annotations

import pytest

from tests.support.fakes import StubReceiver
from xtr_messenger import AckReceiptStamp, Envelope, ErrorDetailsStamp
from xtr_messenger.transport.receiver.chained_receiver import ChainedReceiver

pytestmark = pytest.mark.anyio


async def drain(receiver: ChainedReceiver) -> list[Envelope]:
    return [envelope async for envelope in receiver.get()]


async def test_it_drains_its_receivers_in_order() -> None:
    first = StubReceiver([Envelope("a"), Envelope("b")])
    second = StubReceiver([Envelope("c")])
    chained = ChainedReceiver([first, second])

    collected = await drain(chained)

    assert [envelope.message for envelope in collected] == ["a", "b", "c"]


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
