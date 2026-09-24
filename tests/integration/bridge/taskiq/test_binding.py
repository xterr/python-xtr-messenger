"""End-to-end over a real taskiq broker: publish, bind, dispatch, handle.

A ``TaskiqSender`` publishes; ``bind_bus`` registers a task per declared
message that rebuilds the envelope and dispatches it into a real
``MessageBus``; the bus runs the handlers. Nothing here fakes the bus, the
serializer or the broker — only the transport underneath is in-memory.
"""

from __future__ import annotations

import contextlib
from typing import TYPE_CHECKING

import pytest
from taskiq import InMemoryBroker, SmartRetryMiddleware

from tests.support.messages import IngestDocument, ingest_document
from xtr_messenger import (
    Envelope,
    HandleMessageMiddleware,
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
from xtr_messenger.bridge.taskiq import TaskiqSender, bind_bus
from xtr_messenger.bridge.taskiq.broker import forget_started
from xtr_messenger.bridge.taskiq.labels import RETRIES_LABEL
from xtr_messenger.message_registry import declared_names

if TYPE_CHECKING:
    from collections.abc import Iterator

pytestmark = pytest.mark.anyio

_INGEST = "test.ingest.v1"


@pytest.fixture
def broker() -> Iterator[InMemoryBroker]:
    made = InMemoryBroker(await_inplace=True)
    yield made
    forget_started(made)


@pytest.fixture
def registry() -> HandlersLocator:
    return HandlersLocator()


def _bind(
    broker: InMemoryBroker,
    registry: HandlersLocator,
    serializer: JsonSerializer | None = None,
) -> tuple[str, ...]:
    """Bind ``broker`` to a bus handling from ``registry``, as a worker does."""
    return bind_bus(broker, MessageBus([HandleMessageMiddleware(registry)]), serializer)


def _publisher(broker: InMemoryBroker, serializer: JsonSerializer | None = None) -> MessageBus:
    sender = TaskiqSender(broker, serializer=serializer)
    routing = SendersLocator({IngestDocument: "q"}, {"q": sender})
    return MessageBus([SendMessageMiddleware(routing)])


async def test_a_dispatched_message_reaches_its_handler_fully_typed(
    broker: InMemoryBroker,
    registry: HandlersLocator,
) -> None:
    received: list[IngestDocument] = []

    @as_message_handler(IngestDocument, registry)
    async def ingest(message: IngestDocument) -> None:
        received.append(message)

    _ = _bind(broker, registry)
    message = ingest_document()
    _ = await _publisher(broker).dispatch(message)

    assert received == [message]


async def test_every_handler_of_a_message_runs(
    broker: InMemoryBroker,
    registry: HandlersLocator,
) -> None:
    """One task per message, dispatching into a bus that runs them all.

    A task per *handler*, under the message's name, meant the second
    registration replaced the first and only the last handler ever ran.
    """
    ran: list[str] = []

    @as_message_handler(IngestDocument, registry)
    async def first(message: IngestDocument) -> None:
        del message
        ran.append("first")

    @as_message_handler(IngestDocument, registry)
    async def second(message: IngestDocument) -> None:
        del message
        ran.append("second")

    _ = _bind(broker, registry)
    _ = await _publisher(broker).dispatch(ingest_document())

    assert ran == ["first", "second"]


async def test_binding_reports_the_task_names_it_registered(
    broker: InMemoryBroker,
    registry: HandlersLocator,
) -> None:
    names = _bind(broker, registry)

    assert _INGEST in names
    assert names == declared_names()


async def test_a_handler_module_needs_no_broker_to_declare_itself(
    broker: InMemoryBroker,
    registry: HandlersLocator,
) -> None:
    @as_message_handler(IngestDocument, registry)
    async def ingest(message: IngestDocument) -> None:
        del message

    assert registry.message_types() == (IngestDocument,)
    assert _INGEST not in broker.get_all_tasks()

    _ = _bind(broker, registry)

    assert _INGEST in broker.get_all_tasks()


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

    _ = _bind(broker, registry)
    _ = await _publisher(broker).dispatch(ingest_document())

    assert attempts == [0]


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

    _ = _bind(broker, registry, serializer)
    _ = await _publisher(broker, serializer).dispatch(
        ingest_document(),
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

    _ = _bind(broker, registry)
    result = await _publisher(broker).dispatch(ingest_document())

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

    _ = _bind(broker, registry)
    task = broker.find_task(_INGEST)
    assert task is not None

    handle = await task.kicker().with_labels(**{RETRIES_LABEL: 0}).kiq("{}")
    result = await handle.wait_result()

    assert result.is_err
    assert isinstance(result.error, MessageDecodingFailedError)
    assert ran == []


async def test_retries_are_reported_to_the_handler_as_they_climb() -> None:
    """With a retry ladder the attempt count a handler reads climbs each try."""
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

    _ = _bind(broker, registry)
    with contextlib.suppress(Exception):
        _ = await _publisher(broker).dispatch(ingest_document())

    forget_started(broker)
    assert attempts == [0, 1, 2]
