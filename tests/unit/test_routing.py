from __future__ import annotations

from dataclasses import dataclass
from uuid import uuid4

import pytest

from message_bus import (
    Envelope,
    InMemoryTransport,
    MessageBus,
    NoSenderForMessageError,
    ReceivedStamp,
    SendersLocator,
    SendMessageMiddleware,
    SentStamp,
    TransportMessageIdStamp,
    TransportNamesStamp,
    UnknownTransportError,
    as_message,
    transports_of,
)
from tests.conftest import AnalyseDocument, IngestDocument, UnroutedMessage

pytestmark = pytest.mark.anyio


@dataclass(frozen=True, slots=True)
class BaseEvent:
    pass


@dataclass(frozen=True, slots=True)
class DerivedEvent(BaseEvent):
    pass


@as_message(name="test.self_routing.v1", transport="declared")
@dataclass(frozen=True, slots=True)
class SelfRoutingMessage:
    pass


@as_message(name="test.fanned_out.v1", transport=["first", "second"])
@dataclass(frozen=True, slots=True)
class FannedOutMessage:
    pass


@dataclass(frozen=True, slots=True)
class InheritsRouting(SelfRoutingMessage):
    pass


def ingest_message() -> IngestDocument:
    return IngestDocument(document_id=uuid4(), tenant_id=uuid4())


def test_a_route_naming_an_unregistered_transport_fails_at_construction() -> None:
    with pytest.raises(UnknownTransportError) as excinfo:
        SendersLocator({IngestDocument: "nowhere"}, {"async": InMemoryTransport()})

    assert excinfo.value.transport_name == "nowhere"
    assert excinfo.value.known_transports == ("async",)


async def test_a_routed_message_reaches_its_transport() -> None:
    transport = InMemoryTransport()
    table = SendersLocator({IngestDocument: "async"}, {"async": transport})
    bus = MessageBus([SendMessageMiddleware(table)])
    message = ingest_message()

    await bus.dispatch(message)

    assert transport.messages == (message,)


async def test_a_message_fans_out_to_every_routed_transport() -> None:
    primary, mirror = InMemoryTransport(), InMemoryTransport()
    table = SendersLocator(
        {IngestDocument: ["primary", "mirror"]},
        {"primary": primary, "mirror": mirror},
    )
    bus = MessageBus([SendMessageMiddleware(table)])

    await bus.dispatch(ingest_message())

    assert len(primary.messages) == 1
    assert len(mirror.messages) == 1


async def test_sending_stamps_the_envelope_with_the_transport_alias() -> None:
    table = SendersLocator({IngestDocument: "async"}, {"async": InMemoryTransport()})
    bus = MessageBus([SendMessageMiddleware(table)])

    result = await bus.dispatch(ingest_message())

    sent = result.last(SentStamp)
    assert sent is not None
    assert sent.sender_alias == "async"
    assert sent.sender_class == "InMemoryTransport"
    assert result.last(TransportMessageIdStamp) is not None


async def test_a_base_class_route_covers_its_subclasses() -> None:
    transport = InMemoryTransport()
    table = SendersLocator({BaseEvent: "async"}, {"async": transport})
    bus = MessageBus([SendMessageMiddleware(table)])

    await bus.dispatch(DerivedEvent())

    assert len(transport.messages) == 1


async def test_a_specific_route_wins_over_the_wildcard() -> None:
    specific, catch_all = InMemoryTransport(), InMemoryTransport()
    table = SendersLocator(
        {IngestDocument: "specific", "*": "catch_all"},
        {"specific": specific, "catch_all": catch_all},
    )
    bus = MessageBus([SendMessageMiddleware(table)])

    await bus.dispatch(ingest_message())

    assert len(specific.messages) == 1
    assert catch_all.messages == ()


async def test_the_wildcard_catches_anything_not_routed_explicitly() -> None:
    specific, catch_all = InMemoryTransport(), InMemoryTransport()
    table = SendersLocator(
        {IngestDocument: "specific", "*": "catch_all"},
        {"specific": specific, "catch_all": catch_all},
    )
    bus = MessageBus([SendMessageMiddleware(table)])

    await bus.dispatch(AnalyseDocument(document_id=uuid4()))

    assert specific.messages == ()
    assert len(catch_all.messages) == 1


async def test_a_transport_names_stamp_overrides_the_table() -> None:
    routed, forced = InMemoryTransport(), InMemoryTransport()
    table = SendersLocator(
        {IngestDocument: "routed"},
        {"routed": routed, "forced": forced},
    )
    bus = MessageBus([SendMessageMiddleware(table)])

    await bus.dispatch(ingest_message(), TransportNamesStamp(("forced",)))

    assert routed.messages == ()
    assert len(forced.messages) == 1


async def test_an_unrouted_message_falls_through_the_chain_by_default() -> None:
    transport = InMemoryTransport()
    table = SendersLocator({IngestDocument: "async"}, {"async": transport})
    bus = MessageBus([SendMessageMiddleware(table)])

    result = await bus.dispatch(UnroutedMessage("orphan"))

    assert transport.messages == ()
    assert result.last(SentStamp) is None


async def test_an_unrouted_message_can_be_made_an_error() -> None:
    table = SendersLocator({IngestDocument: "async"}, {"async": InMemoryTransport()})
    bus = MessageBus([SendMessageMiddleware(table, require_sender=True)])

    with pytest.raises(NoSenderForMessageError) as excinfo:
        await bus.dispatch(UnroutedMessage("orphan"))

    assert excinfo.value.message_type is UnroutedMessage
    assert "IngestDocument" in excinfo.value.routed_types


async def test_a_message_can_declare_its_own_transport() -> None:
    declared = InMemoryTransport()
    table = SendersLocator({}, {"declared": declared})
    bus = MessageBus([SendMessageMiddleware(table)])

    await bus.dispatch(SelfRoutingMessage())

    assert len(declared.messages) == 1


async def test_a_message_can_declare_several_transports() -> None:
    first, second = InMemoryTransport(), InMemoryTransport()
    table = SendersLocator({}, {"first": first, "second": second})
    bus = MessageBus([SendMessageMiddleware(table)])

    await bus.dispatch(FannedOutMessage())

    assert len(first.messages) == 1
    assert len(second.messages) == 1


async def test_the_routing_table_overrides_what_a_message_declares() -> None:
    declared, override = InMemoryTransport(), InMemoryTransport()
    table = SendersLocator(
        {SelfRoutingMessage: "override"},
        {"declared": declared, "override": override},
    )
    bus = MessageBus([SendMessageMiddleware(table)])

    await bus.dispatch(SelfRoutingMessage())

    assert declared.messages == ()
    assert len(override.messages) == 1


async def test_the_wildcard_also_overrides_what_a_message_declares() -> None:
    declared, catch_all = InMemoryTransport(), InMemoryTransport()
    table = SendersLocator({"*": "catch_all"}, {"declared": declared, "catch_all": catch_all})
    bus = MessageBus([SendMessageMiddleware(table)])

    await bus.dispatch(SelfRoutingMessage())

    assert declared.messages == ()
    assert len(catch_all.messages) == 1


async def test_a_declared_transport_that_was_never_registered_fails_loudly() -> None:
    table = SendersLocator({}, {"elsewhere": InMemoryTransport()})
    bus = MessageBus([SendMessageMiddleware(table)])

    with pytest.raises(UnknownTransportError, match="declared"):
        await bus.dispatch(SelfRoutingMessage())


def test_a_message_without_a_declared_transport_reports_none() -> None:
    assert transports_of(UnroutedMessage) == ()


def test_a_subclass_inherits_the_transport_its_base_declared() -> None:
    assert transports_of(InheritsRouting) == ("declared",)


async def test_a_received_envelope_is_never_routed_again() -> None:
    transport = InMemoryTransport()
    table = SendersLocator({IngestDocument: "async"}, {"async": transport})
    bus = MessageBus([SendMessageMiddleware(table)])
    envelope = Envelope(ingest_message(), stamps=(ReceivedStamp("async"),))

    await bus.dispatch(envelope)

    assert transport.messages == ()
