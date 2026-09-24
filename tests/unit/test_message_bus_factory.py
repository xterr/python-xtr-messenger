from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, final

import pytest
from typing_extensions import override

from tests.support.fakes import RecordingSender
from tests.support.messages import IngestDocument, ingest_document
from xtr_messenger import (
    HandlersLocator,
    MessageBusConfig,
    MessageBusFactory,
    MiddlewareInterface,
    NoSenderForMessageError,
    ReceivedStamp,
    SenderInterface,
    SentStamp,
    TransportConfig,
    TransportFactoryInterface,
    UnsupportedDsnError,
    as_message_handler,
)

if TYPE_CHECKING:
    from collections.abc import Mapping

    from xtr_messenger import Dsn, Envelope, StackInterface

pytestmark = pytest.mark.anyio


@dataclass(frozen=True, slots=True)
class Unrouted:
    pass


@final
class FakeTransportFactory(TransportFactoryInterface):
    """Builds the given sender for every transport whose DSN scheme it recognises."""

    def __init__(self, scheme: str, sender: SenderInterface) -> None:
        self._scheme = scheme
        self._sender = sender

    @override
    def supports(self, dsn: Dsn) -> bool:
        return dsn.scheme == self._scheme

    @override
    def create(self, group: Mapping[str, TransportConfig]) -> Mapping[str, SenderInterface]:
        return dict.fromkeys(group, self._sender)


@final
class ReceivingSender(SenderInterface):
    """Hands the envelope straight back received, which is all sync:// does."""

    @override
    async def send(self, envelope: Envelope) -> Envelope:
        return envelope.with_stamps(ReceivedStamp("fake"))


@final
class OrderingSender(SenderInterface):
    def __init__(self, log: list[str]) -> None:
        self._log = log

    @override
    async def send(self, envelope: Envelope) -> Envelope:
        self._log.append("send")
        return envelope


@final
class OrderingMiddleware(MiddlewareInterface):
    def __init__(self, log: list[str]) -> None:
        self._log = log

    @override
    async def handle(self, envelope: Envelope, stack: StackInterface, /) -> Envelope:
        self._log.append("middleware")
        return await stack.next().handle(envelope, stack)


async def test_the_factory_routes_by_the_config_map() -> None:
    sender = RecordingSender()
    config = MessageBusConfig(
        transports={"a": TransportConfig("fake://"), "b": TransportConfig("fake://")},
        routing={IngestDocument: "a"},
    )

    result = (
        await MessageBusFactory(config, [FakeTransportFactory("fake", sender)])
        .bus()
        .dispatch(ingest_document())
    )

    sent = result.last(SentStamp)
    assert sent is not None
    assert sent.sender_alias == "a"
    assert len(sender.sent) == 1


async def test_a_sender_handing_the_envelope_back_received_gets_it_handled() -> None:
    private = HandlersLocator()
    seen: list[IngestDocument] = []

    @as_message_handler(IngestDocument, private)
    async def handle(message: IngestDocument) -> None:
        seen.append(message)

    config = MessageBusConfig(
        transports={"fake": TransportConfig("fake://")},
        routing={IngestDocument: "fake"},
    )
    bus = MessageBusFactory(
        config, [FakeTransportFactory("fake", ReceivingSender())], handlers=private
    ).bus()
    message = ingest_document()

    _ = await bus.dispatch(message)

    assert seen == [message]


async def test_handle_unrouted_lets_an_unrouted_message_be_handled() -> None:
    private = HandlersLocator()
    seen: list[Unrouted] = []

    @as_message_handler(Unrouted, private)
    async def handle(message: Unrouted) -> None:
        seen.append(message)

    config = MessageBusConfig(transports={"fake": TransportConfig("fake://")})
    bus = MessageBusFactory(
        config, [FakeTransportFactory("fake", RecordingSender())], handlers=private
    ).bus(handle_unrouted=True)
    message = Unrouted()

    _ = await bus.dispatch(message)

    assert seen == [message]


async def test_an_unrouted_message_is_not_handled_by_default() -> None:
    private = HandlersLocator()
    seen: list[Unrouted] = []

    @as_message_handler(Unrouted, private)
    async def handle(message: Unrouted) -> None:
        seen.append(message)

    config = MessageBusConfig(transports={"fake": TransportConfig("fake://")})
    bus = MessageBusFactory(
        config, [FakeTransportFactory("fake", RecordingSender())], handlers=private
    ).bus()

    _ = await bus.dispatch(Unrouted())

    assert seen == []


async def test_require_sender_rejects_an_unrouted_message() -> None:
    config = MessageBusConfig(transports={"fake": TransportConfig("fake://")})
    bus = MessageBusFactory(config, [FakeTransportFactory("fake", RecordingSender())]).bus(
        require_sender=True
    )

    with pytest.raises(NoSenderForMessageError) as excinfo:
        _ = await bus.dispatch(Unrouted())

    assert excinfo.value.message_type is Unrouted


async def test_middleware_passed_to_the_bus_runs_before_the_send() -> None:
    order: list[str] = []
    config = MessageBusConfig(
        transports={"fake": TransportConfig("fake://")},
        routing={IngestDocument: "fake"},
    )
    bus = MessageBusFactory(config, [FakeTransportFactory("fake", OrderingSender(order))]).bus(
        [OrderingMiddleware(order)]
    )

    _ = await bus.dispatch(ingest_document())

    assert order == ["middleware", "send"]


def test_an_unsupported_dsn_names_the_transport_that_failed() -> None:
    config = MessageBusConfig(transports={"weird": TransportConfig("carrier-pigeon://")})

    with pytest.raises(UnsupportedDsnError) as excinfo:
        _ = MessageBusFactory(config, [FakeTransportFactory("fake", RecordingSender())]).bus()

    assert excinfo.value.transport_name == "weird"
    assert excinfo.value.dsn == "carrier-pigeon://"
