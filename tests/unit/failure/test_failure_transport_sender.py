from __future__ import annotations

from datetime import UTC, datetime
from typing import override

import pytest

from tests.support.failure import failure_transport
from xtr_messenger import (
    AckReceiptStamp,
    DelayStamp,
    Envelope,
    ErrorDetailsStamp,
    FailureTransportSender,
    InMemoryTransport,
    InMemoryTransportFactory,
    MessageBusConfig,
    ReceivedStamp,
    RedeliveryStamp,
    SenderInterface,
    SentToFailureTransportStamp,
    TransportConfig,
    TransportLocator,
    TransportMessageIdStamp,
)

pytestmark = pytest.mark.anyio


async def test_it_sends_a_failed_message_as_a_new_one_marked_failed() -> None:
    sender, kept = failure_transport("orders")
    failed = Envelope(
        "x",
        (
            RedeliveryStamp(2),
            DelayStamp(1000),
            TransportMessageIdStamp("9"),
            ErrorDetailsStamp("RuntimeError", "boom"),
        ),
    )

    assert await sender.send(failed, "orders")

    [moved] = kept.sent
    assert moved.last(RedeliveryStamp) is None
    assert moved.last(DelayStamp) is None
    assert moved.all(TransportMessageIdStamp) != (TransportMessageIdStamp("9"),)
    assert moved.last(ErrorDetailsStamp) == ErrorDetailsStamp("RuntimeError", "boom")
    assert moved.last(SentToFailureTransportStamp) == SentToFailureTransportStamp(
        "orders", datetime(2026, 1, 1, 12, tzinfo=UTC)
    )


async def test_a_message_failing_in_the_failure_transport_goes_nowhere() -> None:
    sender, kept = failure_transport("orders")

    assert sender.failure_transport_of("failed") is None
    assert not await sender.send(Envelope("x"), "failed")
    assert kept.sent == ()


async def test_a_message_that_failed_in_a_failure_transport_is_never_sent_on() -> None:
    """Whatever the configuration says: ``failed`` failing to ``dead`` and
    ``dead`` back to ``failed`` cannot pass a message between them."""
    config = MessageBusConfig(
        transports={
            "jobs": TransportConfig("in-memory://"),
            "failed": TransportConfig("in-memory://", failure_transport="dead"),
            "dead": TransportConfig("in-memory://"),
        },
        failure_transport="failed",
    )
    locator = TransportLocator(config, [InMemoryTransportFactory()])
    sender = FailureTransportSender(config, locator)
    first = SentToFailureTransportStamp("jobs", datetime(2025, 12, 31, tzinfo=UTC))

    assert sender.failure_transport_of("dead") == "failed"
    assert not await sender.send(Envelope("x", (first,)), "failed")
    assert not await sender.send(Envelope("x", (first,)), "dead")
    assert await sender.send(Envelope("x"), "jobs")
    dead = locator.get("dead")
    assert isinstance(dead, InMemoryTransport)
    assert dead.sent == ()


async def test_what_only_this_process_stamped_does_not_travel() -> None:
    sender, kept = failure_transport("orders")

    assert await sender.send(Envelope("x", (ReceivedStamp("orders"), AckReceiptStamp(1))), "orders")

    [moved] = kept.sent
    assert moved.last(ReceivedStamp) is None
    assert moved.last(AckReceiptStamp) is None


async def test_a_transport_with_no_failure_transport_sends_nothing() -> None:
    config = MessageBusConfig(transports={"orders": TransportConfig("in-memory://")})
    sender = FailureTransportSender(config, TransportLocator(config))

    assert not await sender.send(Envelope("x"), "orders")


class _Refusing(SenderInterface):
    @override
    async def send(self, envelope: Envelope) -> Envelope:
        raise RuntimeError("down")


async def test_a_send_that_fails_is_reported_not_raised(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    sender, _ = failure_transport("orders")

    def refusing(_self: TransportLocator, _name: str) -> SenderInterface:
        return _Refusing()

    monkeypatch.setattr(TransportLocator, "get", refusing)

    assert not await sender.send(Envelope("x"), "orders")
    assert "could not be sent to the failure transport 'failed'" in caplog.text
