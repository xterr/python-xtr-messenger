from __future__ import annotations

import contextlib
from typing import TYPE_CHECKING
from uuid import uuid4

import pytest
from taskiq import InMemoryBroker, SmartRetryMiddleware

from message_bus import (
    Envelope,
    HandlersLocator,
    JsonSerializer,
    MessageBus,
    MessageDecodingFailedError,
    RedeliveryStamp,
    SendersLocator,
    SendMessageMiddleware,
    TransportMessageIdStamp,
    as_message_handler,
)
from message_bus.bridge.taskiq import (
    MissingTaskRouteError,
    TaskiqSender,
    assert_routes_registered,
    bind_handlers,
)
from message_bus.bridge.taskiq.broker import forget_started
from message_bus.bridge.taskiq.labels import HEADERS_LABEL, RETRIES_LABEL
from tests.bridge_amqp.recording import RecordingMiddleware
from tests.conftest import AnalyseDocument, IngestDocument

if TYPE_CHECKING:
    from collections.abc import Iterator

pytestmark = pytest.mark.anyio


@pytest.fixture
def broker() -> Iterator[InMemoryBroker]:
    instance = InMemoryBroker(await_inplace=True)
    yield instance
    forget_started(instance)


@pytest.fixture
def registry() -> HandlersLocator:
    return HandlersLocator()


def ingest_message() -> IngestDocument:
    return IngestDocument(document_id=uuid4(), tenant_id=uuid4())


def bus_for(broker: InMemoryBroker, serializer: JsonSerializer | None = None) -> MessageBus:
    sender = TaskiqSender(broker, serializer=serializer)
    return MessageBus([SendMessageMiddleware(SendersLocator({IngestDocument: "q"}, {"q": sender}))])


async def test_a_dispatched_message_reaches_its_handler_fully_typed(
    broker: InMemoryBroker,
    registry: HandlersLocator,
) -> None:
    received: list[IngestDocument] = []

    @as_message_handler(IngestDocument, registry)
    async def ingest(message: IngestDocument) -> None:
        received.append(message)

    _ = bind_handlers(broker, registry)
    message = ingest_message()
    await bus_for(broker).dispatch(message)

    assert received == [message]


async def test_binding_reports_the_task_names_it_registered(
    broker: InMemoryBroker,
    registry: HandlersLocator,
) -> None:
    @as_message_handler(IngestDocument, registry)
    async def ingest(message: IngestDocument) -> None:
        del message

    assert bind_handlers(broker, registry) == ("test.ingest.v1",)


async def test_a_handler_module_needs_no_broker_to_declare_itself(
    broker: InMemoryBroker,
    registry: HandlersLocator,
) -> None:
    @as_message_handler(IngestDocument, registry)
    async def ingest(message: IngestDocument) -> None:
        del message

    assert registry.message_types() == (IngestDocument,)
    assert broker.get_all_tasks() == {}

    _ = bind_handlers(broker, registry)

    assert "test.ingest.v1" in broker.get_all_tasks()


async def test_the_handler_can_also_take_the_envelope(
    broker: InMemoryBroker,
    registry: HandlersLocator,
) -> None:
    attempts: list[int] = []

    @as_message_handler(IngestDocument, registry)
    async def ingest(message: IngestDocument, envelope: Envelope) -> None:
        del message
        stamp = envelope.last(RedeliveryStamp)
        assert stamp is not None
        attempts.append(stamp.retry_count)

    _ = bind_handlers(broker, registry)
    await bus_for(broker).dispatch(ingest_message())

    assert attempts == [0]


async def test_publishing_always_marks_the_first_delivery(
    broker: InMemoryBroker,
    registry: HandlersLocator,
) -> None:
    labels: list[dict[str, object]] = []

    @as_message_handler(IngestDocument, registry)
    async def ingest(message: IngestDocument) -> None:
        del message

    _ = bind_handlers(broker, registry)
    _ = broker.add_middlewares(RecordingMiddleware(labels))
    await bus_for(broker).dispatch(ingest_message())

    assert int(str(labels[0][RETRIES_LABEL])) == 0


async def test_the_envelope_headers_ride_along_with_the_message(
    broker: InMemoryBroker,
    registry: HandlersLocator,
) -> None:
    labels: list[dict[str, object]] = []

    @as_message_handler(IngestDocument, registry)
    async def ingest(message: IngestDocument) -> None:
        del message

    _ = bind_handlers(broker, registry)
    _ = broker.add_middlewares(RecordingMiddleware(labels))
    await bus_for(broker).dispatch(ingest_message())

    assert "test.ingest.v1" in str(labels[0][HEADERS_LABEL])


async def test_stamps_survive_the_trip_to_the_handler(
    broker: InMemoryBroker,
    registry: HandlersLocator,
) -> None:
    seen: list[Envelope] = []
    serializer = JsonSerializer(stamp_types=[TransportMessageIdStamp])

    @as_message_handler(IngestDocument, registry)
    async def ingest(message: IngestDocument, envelope: Envelope) -> None:
        del message
        seen.append(envelope)

    _ = bind_handlers(broker, registry, serializer)
    await bus_for(broker, serializer).dispatch(
        ingest_message(),
        TransportMessageIdStamp("upstream-42"),
    )

    assert seen[0].all(TransportMessageIdStamp)[0].message_id == "upstream-42"


async def test_dispatch_reports_the_transport_message_id(
    broker: InMemoryBroker,
    registry: HandlersLocator,
) -> None:
    @as_message_handler(IngestDocument, registry)
    async def ingest(message: IngestDocument) -> None:
        del message

    _ = bind_handlers(broker, registry)
    result = await bus_for(broker).dispatch(ingest_message())

    assert result.last(TransportMessageIdStamp) is not None


async def test_a_payload_that_drifted_from_its_schema_fails_loudly(
    broker: InMemoryBroker,
    registry: HandlersLocator,
) -> None:
    ran: list[str] = []

    @as_message_handler(IngestDocument, registry)
    async def ingest(message: IngestDocument) -> None:
        del message
        ran.append("ran")

    _ = bind_handlers(broker, registry)
    task = broker.find_task("test.ingest.v1")
    assert task is not None

    handle = await task.kicker().with_labels(**{RETRIES_LABEL: 0}).kiq("{}")
    result = await handle.wait_result()

    assert result.is_err
    assert isinstance(result.error, MessageDecodingFailedError)
    assert "missing required field" in str(result.error)
    assert ran == []


def test_startup_check_passes_once_the_handlers_are_bound(
    broker: InMemoryBroker,
    registry: HandlersLocator,
) -> None:
    @as_message_handler(IngestDocument, registry)
    async def ingest(message: IngestDocument) -> None:
        del message

    _ = bind_handlers(broker, registry)

    assert_routes_registered(broker, [IngestDocument])


def test_startup_check_names_a_message_nothing_will_ever_consume(
    broker: InMemoryBroker,
    registry: HandlersLocator,
) -> None:
    @as_message_handler(IngestDocument, registry)
    async def ingest(message: IngestDocument) -> None:
        del message

    _ = bind_handlers(broker, registry)

    with pytest.raises(MissingTaskRouteError) as excinfo:
        assert_routes_registered(broker, [IngestDocument, AnalyseDocument])

    assert excinfo.value.missing == ("test.analyse.v1",)


async def test_retries_are_reported_to_the_handler_as_they_climb() -> None:
    attempts: list[int] = []
    registry = HandlersLocator()
    broker = InMemoryBroker(await_inplace=True).with_middlewares(
        SmartRetryMiddleware(
            default_retry_count=3,
            default_delay=0.0,
            use_delay_exponent=True,
            default_retry_label=True,
        ),
    )

    @as_message_handler(IngestDocument, registry)
    async def ingest(message: IngestDocument, envelope: Envelope) -> None:
        del message
        stamp = envelope.last(RedeliveryStamp)
        assert stamp is not None
        attempts.append(stamp.retry_count)
        raise RuntimeError("keep failing")

    _ = bind_handlers(broker, registry)
    bus = MessageBus(
        [SendMessageMiddleware(SendersLocator({IngestDocument: "q"}, {"q": TaskiqSender(broker)}))],
    )

    with contextlib.suppress(Exception):
        await bus.dispatch(ingest_message())

    forget_started(broker)
    assert attempts == [0, 1, 2]
