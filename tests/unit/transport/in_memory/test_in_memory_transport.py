from __future__ import annotations

from typing import final

import pytest
from typing_extensions import override

from tests.support.messages import ingest_document
from xtr_messenger import (
    AckReceiptStamp,
    EncodedEnvelope,
    Envelope,
    ErrorDetailsStamp,
    JsonSerializer,
    MessageEncodingFailedError,
    ReceivedStamp,
    SerializerInterface,
    TransportMessageIdStamp,
)
from xtr_messenger.transport.in_memory import InMemoryTransport

pytestmark = pytest.mark.anyio


@final
class RecordingSerializer(SerializerInterface):
    """A serializer that records each call, delegating to a real one."""

    def __init__(self) -> None:
        self._inner = JsonSerializer()
        self.encoded: list[Envelope] = []
        self.decoded: list[EncodedEnvelope] = []

    @override
    def encode(self, envelope: Envelope) -> EncodedEnvelope:
        self.encoded.append(envelope)
        return self._inner.encode(envelope)

    @override
    def decode(self, encoded: EncodedEnvelope) -> Envelope:
        self.decoded.append(encoded)
        return self._inner.decode(encoded)


async def test_send_records_the_dispatched_message() -> None:
    transport = InMemoryTransport()
    message = ingest_document()

    _ = await transport.send(Envelope(message))

    assert transport.messages == (message,)


async def test_the_record_is_not_drained_by_receiving() -> None:
    """sent is an append-only log; get() consumes an independent queue, so a
    worker draining the queue never empties the history a test asserts on."""
    transport = InMemoryTransport()
    message = ingest_document()
    _ = await transport.send(Envelope(message))

    _ = [envelope async for envelope in transport.get()]

    assert transport.messages == (message,)
    assert transport.pending == 0


async def test_pending_counts_queued_but_uncollected_envelopes() -> None:
    transport = InMemoryTransport()

    _ = await transport.send(Envelope(ingest_document()))
    _ = await transport.send(Envelope(ingest_document()))

    assert transport.pending == 2


async def test_sent_keeps_the_full_history_in_dispatch_order() -> None:
    transport = InMemoryTransport()
    first, second = ingest_document(), ingest_document()

    _ = await transport.send(Envelope(first))
    _ = await transport.send(Envelope(second))

    assert transport.messages == (first, second)


async def test_clear_forgets_everything() -> None:
    transport = InMemoryTransport()
    _ = await transport.send(Envelope(ingest_document()))

    transport.clear()

    assert transport.messages == ()
    assert transport.pending == 0
    assert transport.rejected == ()


async def test_send_stamps_the_returned_envelope_with_a_message_id() -> None:
    transport = InMemoryTransport()

    result = await transport.send(Envelope(ingest_document()))

    assert result.last(TransportMessageIdStamp) is not None


async def test_the_recorded_envelope_carries_the_message_id() -> None:
    transport = InMemoryTransport()

    _ = await transport.send(Envelope(ingest_document()))

    assert transport.sent[0].last(TransportMessageIdStamp) is not None


async def test_get_stamps_each_envelope_received_with_a_receipt() -> None:
    transport = InMemoryTransport()
    _ = await transport.send(Envelope(ingest_document()))

    collected = [envelope async for envelope in transport.get()]

    assert collected[0].last(ReceivedStamp) == ReceivedStamp("in-memory")
    assert collected[0].last(AckReceiptStamp) == AckReceiptStamp(1)


async def test_get_stops_when_the_queue_is_empty() -> None:
    """It stops rather than waiting, so a test can drive the loop to completion."""
    transport = InMemoryTransport()
    _ = await transport.send(Envelope(ingest_document()))
    _ = await transport.send(Envelope(ingest_document()))

    collected = [envelope async for envelope in transport.get()]

    assert len(collected) == 2


async def test_ack_settles_a_collected_message() -> None:
    transport = InMemoryTransport()
    _ = await transport.send(Envelope(ingest_document()))
    received = [envelope async for envelope in transport.get()]

    await transport.ack(received[0])

    assert transport.rejected == ()


async def test_ack_ignores_an_envelope_that_was_never_collected() -> None:
    transport = InMemoryTransport()

    await transport.ack(Envelope(ingest_document()))

    assert transport.rejected == ()


async def test_reject_records_the_envelope_as_handed_over() -> None:
    """Recorded as handed over, not as collected, so an ErrorDetailsStamp the
    consumer added on the way survives rather than being discarded."""
    transport = InMemoryTransport()
    _ = await transport.send(Envelope(ingest_document()))
    received = [envelope async for envelope in transport.get()]
    marked = received[0].with_stamps(ErrorDetailsStamp("RuntimeError", "boom"))

    await transport.reject(marked)

    assert transport.rejected == (marked,)


async def test_a_given_serializer_round_trips_every_message_on_the_way_in() -> None:
    serializer = RecordingSerializer()
    transport = InMemoryTransport(serializer=serializer)
    message = ingest_document()

    _ = await transport.send(Envelope(message))

    assert len(serializer.encoded) == 1
    assert len(serializer.decoded) == 1
    assert transport.messages == (message,)


async def test_a_serializer_surfaces_a_message_that_cannot_be_encoded() -> None:
    """Round-tripping in a test turns an unserializable message into a test
    failure rather than a production one."""
    transport = InMemoryTransport(serializer=JsonSerializer())

    with pytest.raises(MessageEncodingFailedError):
        _ = await transport.send(Envelope("not a dataclass"))
