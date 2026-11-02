from __future__ import annotations

from typing import final

import pytest
from typing_extensions import override

from tests.support.messages import (
    assert_dispatch_invoice_is_exact,
    dispatch_invoice,
    ingest_document,
)
from tests.support.serialization import json_serializer
from xtr_messenger import (
    Dsn,
    EncodedEnvelope,
    Envelope,
    MissingSerializerError,
    SerializerInterface,
    TransportConfig,
    UnknownTransportOptionError,
    WorkerProvidingInterface,
)
from xtr_messenger.transport.in_memory import InMemoryTransport, InMemoryTransportFactory

pytestmark = pytest.mark.anyio


@final
class SpySerializer(SerializerInterface):
    """A serializer that counts its encodes, delegating to a real one."""

    def __init__(self) -> None:
        self._inner = json_serializer()
        self.encodes = 0

    @override
    def encode(self, envelope: Envelope) -> EncodedEnvelope:
        self.encodes += 1
        return self._inner.encode(envelope)

    @override
    def decode(self, encoded: EncodedEnvelope) -> Envelope:
        return self._inner.decode(encoded)


def test_it_supports_the_in_memory_scheme() -> None:
    assert InMemoryTransportFactory().supports(Dsn.parse("in-memory://"))


def test_it_does_not_support_another_scheme() -> None:
    assert not InMemoryTransportFactory().supports(Dsn.parse("sync://"))


def test_it_builds_an_in_memory_transport() -> None:
    built = InMemoryTransportFactory().create({"test": TransportConfig("in-memory://")})

    assert isinstance(built["test"], InMemoryTransport)


def test_it_reuses_the_transport_built_for_a_name() -> None:
    """The asserting code, the publishing code, and any worker must hold the
    same recorder, or each gets a private queue and all three break."""
    factory = InMemoryTransportFactory()
    group = {"test": TransportConfig("in-memory://")}

    first = factory.create(group)
    second = factory.create(group)

    assert first["test"] is second["test"]


def test_it_refuses_an_unknown_option() -> None:
    group = {"test": TransportConfig("in-memory://?bogus=1")}

    with pytest.raises(UnknownTransportOptionError):
        _ = InMemoryTransportFactory().create(group)


async def test_serialize_true_round_trips_through_the_given_serializer() -> None:
    serializer = SpySerializer()
    factory = InMemoryTransportFactory(serializer=serializer)
    built = factory.create({"test": TransportConfig("in-memory://?serialize=true")})

    _ = await built["test"].send(Envelope(ingest_document()))

    assert serializer.encodes == 1


async def test_serialize_true_carries_a_message_exactly() -> None:
    factory = InMemoryTransportFactory(serializer=json_serializer())
    transport = factory.create({"test": TransportConfig("in-memory://?serialize=true")})["test"]
    assert isinstance(transport, InMemoryTransport)
    sent = dispatch_invoice()

    _ = await transport.send(Envelope(sent))

    assert_dispatch_invoice_is_exact(transport.messages[0], sent)


def test_serialize_true_without_a_serializer_is_refused_naming_the_setting() -> None:
    group = {"test": TransportConfig("in-memory://?serialize=true")}

    with pytest.raises(MissingSerializerError, match="serialize") as raised:
        _ = InMemoryTransportFactory().create(group)

    assert raised.value.scheme == "in-memory"


async def test_without_serialize_the_transport_records_without_round_tripping() -> None:
    """A recorder with no serializer keeps a message that has no wire form."""
    built = InMemoryTransportFactory().create({"test": TransportConfig("in-memory://")})
    transport = built["test"]
    assert isinstance(transport, InMemoryTransport)

    _ = await transport.send(Envelope("not a dataclass"))

    assert transport.messages == ("not a dataclass",)


def test_it_does_not_bring_its_own_worker() -> None:
    assert not isinstance(InMemoryTransportFactory(), WorkerProvidingInterface)
