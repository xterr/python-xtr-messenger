from __future__ import annotations

from message_bus import (
    AckReceiptStamp,
    DelayStamp,
    ErrorDetailsStamp,
    HandledStamp,
    ReceivedStamp,
    RedeliveryStamp,
    SentStamp,
    StampInterface,
    TransportMessageIdStamp,
    TransportNamesStamp,
)

EVERY_STAMP: tuple[object, ...] = (
    AckReceiptStamp(1),
    DelayStamp(500),
    ErrorDetailsStamp("RuntimeError", "boom"),
    HandledStamp("handler"),
    ReceivedStamp("async"),
    RedeliveryStamp(0),
    SentStamp("InMemoryTransport", "async"),
    TransportMessageIdStamp("abc"),
    TransportNamesStamp(("async",)),
)

NOT_STAMPS: tuple[object, ...] = ("a string", 42, None, object())


def test_every_shipped_stamp_satisfies_the_contract() -> None:
    for stamp in EVERY_STAMP:
        assert isinstance(stamp, StampInterface)


def test_no_arbitrary_object_satisfies_the_contract() -> None:
    """Nominal, not structural: a Protocol with no members would match every object."""
    for value in NOT_STAMPS:
        assert not isinstance(value, StampInterface)
