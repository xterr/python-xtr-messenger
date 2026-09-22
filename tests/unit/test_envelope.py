from __future__ import annotations

from dataclasses import dataclass

from message_bus import (
    DelayStamp,
    Envelope,
    NonSendableStampInterface,
    ReceivedStamp,
    SentStamp,
    StampInterface,
    TransportMessageIdStamp,
)


@dataclass(frozen=True, slots=True)
class Marker(StampInterface):
    value: str


def test_with_stamps_returns_a_new_envelope_and_leaves_the_original_alone() -> None:
    original = Envelope("payload")

    stamped = original.with_stamps(Marker("first"))

    assert original.stamps == ()
    assert stamped.stamps == (Marker("first"),)
    assert stamped is not original


def test_with_stamps_appends_in_order() -> None:
    envelope = Envelope("payload").with_stamps(Marker("a")).with_stamps(Marker("b"))

    assert envelope.all(Marker) == (Marker("a"), Marker("b"))


def test_last_returns_the_most_recent_stamp_of_a_type() -> None:
    envelope = Envelope("payload").with_stamps(Marker("old"), Marker("new"))

    assert envelope.last(Marker) == Marker("new")


def test_last_returns_none_when_the_stamp_is_absent() -> None:
    assert Envelope("payload").last(Marker) is None


def test_with_stamps_without_arguments_returns_the_same_instance() -> None:
    envelope = Envelope("payload")

    assert envelope.with_stamps() is envelope


def test_without_stamps_removes_subclasses_too() -> None:
    envelope = Envelope("payload").with_stamps(
        Marker("kept"),
        SentStamp("T", "async"),
        ReceivedStamp("async"),
    )

    stripped = envelope.without_stamps(NonSendableStampInterface)

    assert stripped.stamps == (Marker("kept"),)


def test_without_stamps_returns_the_same_instance_when_nothing_matches() -> None:
    envelope = Envelope("payload").with_stamps(Marker("kept"))

    assert envelope.without_stamps(ReceivedStamp) is envelope


def test_wrap_passes_an_existing_envelope_through_with_its_stamps() -> None:
    envelope = Envelope("payload", stamps=(Marker("existing"),))

    assert Envelope.wrap(envelope) is envelope


def test_wrap_adds_stamps_to_an_existing_envelope_without_mutating_it() -> None:
    envelope = Envelope("payload", stamps=(Marker("existing"),))

    rewrapped = Envelope.wrap(envelope, [Marker("added")])

    assert envelope.stamps == (Marker("existing"),)
    assert rewrapped.all(Marker) == (Marker("existing"), Marker("added"))


def test_wrap_puts_a_raw_message_in_a_fresh_envelope() -> None:
    wrapped = Envelope.wrap("payload")

    assert wrapped.message == "payload"
    assert wrapped.stamps == ()


def test_stripping_a_stamp_family_keeps_everything_outside_it() -> None:
    """A marker Protocol would match every object and strip the lot."""
    envelope = Envelope("payload").with_stamps(
        DelayStamp(500),
        TransportMessageIdStamp("abc"),
        SentStamp("T", "async"),
        ReceivedStamp("async"),
    )

    stripped = envelope.without_stamps(NonSendableStampInterface)

    assert [type(s).__name__ for s in stripped.stamps] == [
        "DelayStamp",
        "TransportMessageIdStamp",
    ]


def test_only_real_stamps_satisfy_the_stamp_contract() -> None:
    """Nominal, not structural: arbitrary objects are not stamps."""
    assert isinstance(DelayStamp(1), StampInterface)
    assert not isinstance("a string", StampInterface)
    assert not isinstance(42, StampInterface)
    assert not isinstance(None, StampInterface)


def test_a_sendable_stamp_is_not_treated_as_non_sendable() -> None:
    assert not isinstance(DelayStamp(1), NonSendableStampInterface)
    assert isinstance(SentStamp("T", "async"), NonSendableStampInterface)
