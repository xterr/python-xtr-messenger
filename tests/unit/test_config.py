from __future__ import annotations

from typing import final
from uuid import uuid4

import pytest

from message_bus import (
    Envelope,
    InMemoryTransport,
    InMemoryTransportFactory,
    MessageBusConfig,
    MessageBusFactory,
    SentStamp,
    StackInterface,
    SyncTransport,
    SyncTransportFactory,
    TransportConfig,
    UnknownTransportNameError,
    UnsupportedDsnError,
    WorkerFactory,
    as_message_handler,
)
from tests.conftest import AnalyseDocument, IngestDocument

pytestmark = pytest.mark.anyio

AMQP = "amqp://guest:guest@localhost:5672/"


def ingest_message() -> IngestDocument:
    return IngestDocument(document_id=uuid4(), tenant_id=uuid4())


def test_a_config_builds_nothing_by_itself() -> None:
    config = MessageBusConfig(transports={"sync": TransportConfig("sync://")})

    built = {"bus", "senders", "locator", "consumer"} & set(dir(config))

    assert built == set()


def test_a_routing_map_is_optional() -> None:
    assert MessageBusConfig(transports={"sync": TransportConfig("sync://")}).routing == {}


def test_a_dsn_picks_the_adapter() -> None:
    config = MessageBusConfig(transports={"sync": TransportConfig("sync://")})

    built = SyncTransportFactory().create(config.transports)

    assert isinstance(built["sync"], SyncTransport)


def test_the_in_memory_dsn_picks_the_recording_adapter() -> None:
    config = MessageBusConfig(transports={"test": TransportConfig("in-memory://")})

    built = InMemoryTransportFactory().create(config.transports)

    assert isinstance(built["test"], InMemoryTransport)


def test_an_unrecognised_dsn_names_the_transport_that_failed() -> None:
    config = MessageBusConfig(transports={"weird": TransportConfig("carrier-pigeon://")})

    with pytest.raises(UnsupportedDsnError) as excinfo:
        _ = MessageBusFactory(config).bus()

    assert excinfo.value.transport_name == "weird"


async def test_the_factory_routes_by_the_config_map() -> None:
    config = MessageBusConfig(
        transports={"a": TransportConfig("in-memory://"), "b": TransportConfig("in-memory://")},
        routing={IngestDocument: "a", AnalyseDocument: "b"},
    )

    result = await MessageBusFactory(config).bus().dispatch(ingest_message())

    sent = result.last(SentStamp)
    assert sent is not None
    assert sent.sender_alias == "a"


async def test_a_sync_transport_built_from_a_dsn_reaches_a_declared_handler() -> None:
    seen: list[IngestDocument] = []

    @as_message_handler(IngestDocument)
    async def ingest(message: IngestDocument) -> None:
        seen.append(message)

    config = MessageBusConfig(
        transports={"sync": TransportConfig("sync://")},
        routing={IngestDocument: "sync"},
    )
    message = ingest_message()

    await MessageBusFactory(config).bus().dispatch(message)

    assert message in seen


async def test_middleware_passed_to_bus_runs_before_the_send() -> None:
    order: list[str] = []

    @final
    class Recording:
        async def handle(self, envelope: Envelope, stack: StackInterface, /) -> Envelope:
            order.append("middleware")
            return await stack.next().handle(envelope, stack)

    config = MessageBusConfig(
        transports={"test": TransportConfig("in-memory://")},
        routing={IngestDocument: "test"},
    )

    _ = await MessageBusFactory(config).bus([Recording()]).dispatch(ingest_message())

    assert order == ["middleware"]


def test_asking_for_an_undefined_transport_names_what_exists() -> None:
    config = MessageBusConfig(transports={"high": TransportConfig(AMQP, queue="jobs_high")})

    with pytest.raises(UnknownTransportNameError) as excinfo:
        _ = WorkerFactory(config).worker(["nope"])

    assert excinfo.value.requested == ("nope",)
    assert excinfo.value.defined == ("high",)
