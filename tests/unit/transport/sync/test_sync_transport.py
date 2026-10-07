from __future__ import annotations

import pytest

from tests.support.messages import ingest_document
from xtr_messenger import (
    DelayStamp,
    Envelope,
    HandledStamp,
    ReceivedStamp,
    SentStamp,
    SyncTransport,
    UnsupportedStampError,
)

pytestmark = pytest.mark.anyio


async def test_send_marks_the_envelope_received_under_the_last_sent_alias() -> None:
    """It calls no handler: it marks the envelope received and hands it back,
    so the bus does the handling in one place."""
    envelope = Envelope(ingest_document()).with_stamps(SentStamp("SyncTransport", "orders"))

    result = await SyncTransport().send(envelope)

    assert result.last(ReceivedStamp) == ReceivedStamp("orders")


async def test_send_falls_back_to_sync_when_no_sent_stamp_is_present() -> None:
    result = await SyncTransport().send(Envelope(ingest_document()))

    assert result.last(ReceivedStamp) == ReceivedStamp("sync")


async def test_send_uses_the_alias_of_the_last_sent_stamp() -> None:
    envelope = Envelope(ingest_document()).with_stamps(
        SentStamp("SyncTransport", "first"),
        SentStamp("SyncTransport", "second"),
    )

    result = await SyncTransport().send(envelope)

    assert result.last(ReceivedStamp) == ReceivedStamp("second")


async def test_send_calls_no_handler() -> None:
    result = await SyncTransport().send(Envelope(ingest_document()))

    assert result.last(HandledStamp) is None


async def test_the_receive_half_yields_nothing() -> None:
    """A worker pointed here starts, finds nothing outstanding, and stops."""
    collected = [envelope async for envelope in SyncTransport().get()]

    assert collected == []


async def test_ack_is_a_no_op() -> None:
    transport = SyncTransport()

    await transport.ack(Envelope(ingest_document()))


async def test_reject_is_a_no_op() -> None:
    transport = SyncTransport()

    await transport.reject(Envelope(ingest_document()))


async def test_a_delayed_envelope_is_refused_rather_than_sent_immediately() -> None:
    """A sync transport handles at once, so it cannot honour a delay: refusing
    surfaces the mismatch instead of dropping the request silently."""
    envelope = Envelope(ingest_document()).with_stamps(DelayStamp(1000))

    with pytest.raises(UnsupportedStampError) as excinfo:
        _ = await SyncTransport().send(envelope)

    assert excinfo.value.stamp_name == "DelayStamp"
    assert excinfo.value.transport == "sync"
