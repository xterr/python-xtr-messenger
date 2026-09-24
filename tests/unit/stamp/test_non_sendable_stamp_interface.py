from __future__ import annotations

from message_bus import (
    AckReceiptStamp,
    DelayStamp,
    HandledStamp,
    NonSendableStampInterface,
    ReceivedStamp,
    RedeliveryStamp,
    SentStamp,
    StampInterface,
)

NON_SENDABLE: tuple[object, ...] = (
    SentStamp("InMemoryTransport", "async"),
    ReceivedStamp("async"),
    HandledStamp("handler"),
    AckReceiptStamp(1),
)

SENDABLE: tuple[object, ...] = (
    DelayStamp(500),
    RedeliveryStamp(0),
)


def test_process_local_stamps_are_marked_non_sendable() -> None:
    for stamp in NON_SENDABLE:
        assert isinstance(stamp, NonSendableStampInterface)


def test_wire_stamps_are_not_marked_non_sendable() -> None:
    for stamp in SENDABLE:
        assert not isinstance(stamp, NonSendableStampInterface)


def test_a_non_sendable_stamp_is_still_a_stamp() -> None:
    """The family narrows the contract; it does not replace it."""
    for stamp in NON_SENDABLE:
        assert isinstance(stamp, StampInterface)
