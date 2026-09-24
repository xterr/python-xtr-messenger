from __future__ import annotations

from dataclasses import dataclass
from uuid import uuid4

import pytest

from message_bus import Envelope, TransportNamesStamp, UnknownTransportError, as_message
from message_bus.transport.sender import SendersLocator
from tests.support.fakes import RecordingSender
from tests.support.messages import AnalyseDocument, IngestDocument, ingest_document


@dataclass(frozen=True, slots=True)
class BaseEvent:
    pass


@dataclass(frozen=True, slots=True)
class DerivedEvent(BaseEvent):
    pass


@as_message(name="test.unit.sender.self_routing.v1", transport="declared")
@dataclass(frozen=True, slots=True)
class SelfRoutingMessage:
    pass


@as_message(name="test.unit.sender.fanned_out.v1", transport=["first", "second"])
@dataclass(frozen=True, slots=True)
class FannedOutMessage:
    pass


def names_for(locator: SendersLocator, envelope: Envelope) -> list[str]:
    return [name for name, _ in locator.senders_for(envelope)]


def test_a_route_naming_an_unknown_transport_fails_at_construction() -> None:
    with pytest.raises(UnknownTransportError) as excinfo:
        _ = SendersLocator({IngestDocument: "nowhere"}, {"async": RecordingSender()})

    assert excinfo.value.names == ("nowhere",)
    assert excinfo.value.known == ("async",)


def test_a_transport_names_stamp_overrides_the_table() -> None:
    locator = SendersLocator(
        {IngestDocument: "routed"},
        {"routed": RecordingSender(), "forced": RecordingSender()},
    )
    envelope = Envelope(ingest_document()).with_stamps(TransportNamesStamp(("forced",)))

    assert names_for(locator, envelope) == ["forced"]


def test_the_table_covers_a_subclass_through_its_base() -> None:
    """Routing a base class routes every subclass."""
    locator = SendersLocator({BaseEvent: "async"}, {"async": RecordingSender()})

    assert names_for(locator, Envelope(DerivedEvent())) == ["async"]


def test_a_specific_route_wins_over_the_wildcard() -> None:
    locator = SendersLocator(
        {IngestDocument: "specific", "*": "catch_all"},
        {"specific": RecordingSender(), "catch_all": RecordingSender()},
    )

    assert names_for(locator, Envelope(ingest_document())) == ["specific"]


def test_the_wildcard_catches_anything_not_routed_explicitly() -> None:
    locator = SendersLocator(
        {IngestDocument: "specific", "*": "catch_all"},
        {"specific": RecordingSender(), "catch_all": RecordingSender()},
    )

    assert names_for(locator, Envelope(AnalyseDocument(document_id=uuid4()))) == ["catch_all"]


def test_a_message_declares_its_own_transport_as_a_last_resort() -> None:
    locator = SendersLocator({}, {"declared": RecordingSender()})

    assert names_for(locator, Envelope(SelfRoutingMessage())) == ["declared"]


def test_a_message_can_declare_several_transports() -> None:
    locator = SendersLocator({}, {"first": RecordingSender(), "second": RecordingSender()})

    assert names_for(locator, Envelope(FannedOutMessage())) == ["first", "second"]


def test_the_table_overrides_what_a_message_declares() -> None:
    """An application can re-route a message it does not own."""
    locator = SendersLocator(
        {SelfRoutingMessage: "override"},
        {"declared": RecordingSender(), "override": RecordingSender()},
    )

    assert names_for(locator, Envelope(SelfRoutingMessage())) == ["override"]


def test_the_wildcard_overrides_what_a_message_declares() -> None:
    locator = SendersLocator(
        {"*": "catch_all"},
        {"declared": RecordingSender(), "catch_all": RecordingSender()},
    )

    assert names_for(locator, Envelope(SelfRoutingMessage())) == ["catch_all"]


def test_a_route_can_fan_out_to_several_transports() -> None:
    locator = SendersLocator(
        {IngestDocument: ["primary", "mirror"]},
        {"primary": RecordingSender(), "mirror": RecordingSender()},
    )

    assert names_for(locator, Envelope(ingest_document())) == ["primary", "mirror"]


def test_a_transport_named_twice_is_yielded_once() -> None:
    locator = SendersLocator({IngestDocument: ["dup", "dup"]}, {"dup": RecordingSender()})

    assert names_for(locator, Envelope(ingest_document())) == ["dup"]


def test_a_declared_transport_that_was_never_registered_fails_at_lookup() -> None:
    """Construction validates routes; a declared transport is only checked when
    a message that declares it is resolved."""
    locator = SendersLocator({}, {"elsewhere": RecordingSender()})

    with pytest.raises(UnknownTransportError) as excinfo:
        _ = names_for(locator, Envelope(SelfRoutingMessage()))

    assert excinfo.value.names == ("declared",)
    assert excinfo.value.known == ("elsewhere",)


def test_routed_type_names_lists_the_routed_types_readably() -> None:
    locator = SendersLocator({IngestDocument: "a", "*": "a"}, {"a": RecordingSender()})

    assert locator.routed_type_names() == ("*", "IngestDocument")
