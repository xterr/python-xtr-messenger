"""Registering a task per declared message, each dispatching into a bus."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pytest
from taskiq import InMemoryBroker
from xtr_event_dispatcher import EventDispatcher

from tests.support.fakes import RecordingBus
from tests.support.messages import ingest_document
from xtr_messenger import (
    Envelope,
    ErrorDetailsStamp,
    JsonSerializer,
    MessageDecodingFailedError,
    ReceivedStamp,
    RedeliveryStamp,
)
from xtr_messenger.bridge.taskiq.binding import bind_bus
from xtr_messenger.bridge.taskiq.broker import forget_started
from xtr_messenger.bridge.taskiq.labels import HEADERS_LABEL, QUEUE_LABEL, RETRIES_LABEL
from xtr_messenger.event import (
    AbstractWorkerMessageEvent,
    WorkerMessageFailedEvent,
    WorkerMessageHandledEvent,
    WorkerMessageReceivedEvent,
)
from xtr_messenger.message_registry import declared_names

if TYPE_CHECKING:
    from collections.abc import Iterator

    from taskiq import AsyncTaskiqTask

pytestmark = pytest.mark.anyio

_INGEST = "test.ingest.v1"
_LOST = 1_000_000


@pytest.fixture
def broker() -> Iterator[InMemoryBroker]:
    made = InMemoryBroker(await_inplace=True)
    yield made
    forget_started(made)


def _body() -> tuple[str, str]:
    """Return an encoded IngestDocument body and its headers label."""
    encoded = JsonSerializer().encode(Envelope.wrap(ingest_document()))
    return encoded.body, json.dumps(encoded.headers)


async def _kick(
    broker: InMemoryBroker,
    name: str,
    body: str,
    **labels: str | float,
) -> AsyncTaskiqTask[None]:
    task = broker.find_task(name)
    assert task is not None
    return await task.kicker().with_labels(**labels).kiq(body)


async def test_it_registers_a_task_per_declared_name(broker: InMemoryBroker) -> None:
    names = bind_bus(broker, RecordingBus())

    assert names == declared_names()
    assert _INGEST in names
    assert all(name in broker.get_all_tasks() for name in names)


async def test_a_task_rebuilds_the_envelope_and_dispatches_it(broker: InMemoryBroker) -> None:
    bus = RecordingBus()
    _ = bind_bus(broker, bus)
    body, headers = _body()

    _ = await _kick(broker, _INGEST, body, **{RETRIES_LABEL: 0, HEADERS_LABEL: headers})

    [envelope] = bus.dispatched
    assert type(envelope.message).__name__ == "IngestDocument"
    assert envelope.last(ReceivedStamp) == ReceivedStamp("taskiq")
    assert envelope.last(RedeliveryStamp) == RedeliveryStamp(0)


async def test_the_attempt_comes_from_the_retries_label(broker: InMemoryBroker) -> None:
    bus = RecordingBus()
    _ = bind_bus(broker, bus)
    body, headers = _body()

    _ = await _kick(broker, _INGEST, body, **{RETRIES_LABEL: 2, HEADERS_LABEL: headers})

    stamp = bus.dispatched[0].last(RedeliveryStamp)
    assert stamp is not None
    assert stamp.retry_count == 2


async def test_a_missing_retry_label_reads_as_a_very_late_attempt(broker: InMemoryBroker) -> None:
    """The sender always sets ``_retries``; its absence is anomalous, so a
    handler's "is this the final try?" check errs on the safe side."""
    bus = RecordingBus()
    _ = bind_bus(broker, bus)
    body, headers = _body()

    _ = await _kick(broker, _INGEST, body, **{HEADERS_LABEL: headers})

    stamp = bus.dispatched[0].last(RedeliveryStamp)
    assert stamp is not None
    assert stamp.retry_count == _LOST


async def test_an_unreadable_retry_label_reads_as_a_very_late_attempt(
    broker: InMemoryBroker,
) -> None:
    bus = RecordingBus()
    _ = bind_bus(broker, bus)
    body, headers = _body()

    _ = await _kick(broker, _INGEST, body, **{RETRIES_LABEL: "later", HEADERS_LABEL: headers})

    stamp = bus.dispatched[0].last(RedeliveryStamp)
    assert stamp is not None
    assert stamp.retry_count == _LOST


async def test_a_boolean_retry_label_reads_as_a_very_late_attempt(broker: InMemoryBroker) -> None:
    """A boolean is not a count: taskiq round-trips it as one, so it is
    rejected rather than coerced to 0 or 1."""
    bus = RecordingBus()
    _ = bind_bus(broker, bus)
    body, headers = _body()

    _ = await _kick(broker, _INGEST, body, **{RETRIES_LABEL: True, HEADERS_LABEL: headers})

    stamp = bus.dispatched[0].last(RedeliveryStamp)
    assert stamp is not None
    assert stamp.retry_count == _LOST


async def test_a_missing_headers_label_types_by_the_task_name(broker: InMemoryBroker) -> None:
    """Without the headers label the type is taken from the task name, which
    is the message's own name — enough to decode the body."""
    bus = RecordingBus()
    _ = bind_bus(broker, bus)
    body, _headers = _body()

    _ = await _kick(broker, _INGEST, body, **{RETRIES_LABEL: 0})

    assert type(bus.dispatched[0].message).__name__ == "IngestDocument"


async def test_a_malformed_headers_label_fails_loudly(broker: InMemoryBroker) -> None:
    _ = bind_bus(broker, RecordingBus())
    body, _headers = _body()

    handle = await _kick(broker, _INGEST, body, **{RETRIES_LABEL: 0, HEADERS_LABEL: "not json"})
    result = await handle.wait_result()

    assert result.is_err
    assert isinstance(result.error, MessageDecodingFailedError)


async def test_a_bus_failure_propagates_to_taskiq(broker: InMemoryBroker) -> None:
    """A failed dispatch reaches taskiq as an error, so its retry and
    dead-letter machinery can act on it."""
    bus = RecordingBus(failure=RuntimeError("boom"))
    _ = bind_bus(broker, bus)
    body, headers = _body()

    handle = await _kick(broker, _INGEST, body, **{RETRIES_LABEL: 0, HEADERS_LABEL: headers})
    result = await handle.wait_result()

    assert result.is_err
    assert isinstance(result.error, RuntimeError)
    assert len(bus.dispatched) == 1


async def test_binding_twice_lets_the_last_bus_win(broker: InMemoryBroker) -> None:
    """Binding again is harmless: each task replaces the one under its name."""
    first, second = RecordingBus(), RecordingBus()
    names_first = bind_bus(broker, first)
    names_second = bind_bus(broker, second)
    body, headers = _body()

    _ = await _kick(broker, _INGEST, body, **{RETRIES_LABEL: 0, HEADERS_LABEL: headers})

    assert names_first == names_second
    assert first.dispatched == []
    assert len(second.dispatched) == 1


def _names_seen(dispatcher: EventDispatcher) -> list[str]:
    seen: list[str] = []

    def record(event: AbstractWorkerMessageEvent) -> None:
        seen.append(f"{type(event).__name__}:{event.receiver_name}")

    for event_type in (
        WorkerMessageReceivedEvent,
        WorkerMessageHandledEvent,
        WorkerMessageFailedEvent,
    ):
        dispatcher.add_listener(event_type, record)
    return seen


async def test_a_task_announces_its_message_as_the_library_worker_does(
    broker: InMemoryBroker,
) -> None:
    dispatcher = EventDispatcher()
    seen = _names_seen(dispatcher)
    _ = bind_bus(broker, RecordingBus(), event_dispatcher=dispatcher, receiver_names={None: "jobs"})
    body, headers = _body()

    _ = await _kick(broker, _INGEST, body, **{RETRIES_LABEL: 0, HEADERS_LABEL: headers})

    assert seen == ["WorkerMessageReceivedEvent:jobs", "WorkerMessageHandledEvent:jobs"]


async def test_the_received_stamp_names_the_transport_by_its_queue(
    broker: InMemoryBroker,
) -> None:
    bus = RecordingBus()
    _ = bind_bus(broker, bus, receiver_names={None: "default", "urgent": "high"})
    body, headers = _body()

    _ = await _kick(broker, _INGEST, body, **{RETRIES_LABEL: 0, HEADERS_LABEL: headers})
    _ = await _kick(
        broker,
        _INGEST,
        body,
        **{RETRIES_LABEL: 0, HEADERS_LABEL: headers, QUEUE_LABEL: "urgent"},
    )

    assert [e.last(ReceivedStamp) for e in bus.dispatched] == [
        ReceivedStamp("default"),
        ReceivedStamp("high"),
    ]


async def test_a_queue_no_transport_serves_reads_as_taskiq(broker: InMemoryBroker) -> None:
    bus = RecordingBus()
    _ = bind_bus(broker, bus, receiver_names={"a": "first", "b": "second"})
    body, headers = _body()

    _ = await _kick(broker, _INGEST, body, **{RETRIES_LABEL: 0, HEADERS_LABEL: headers})

    assert bus.dispatched[0].last(ReceivedStamp) == ReceivedStamp("taskiq")


async def test_a_skipped_message_returns_normally_without_dispatching(
    broker: InMemoryBroker,
) -> None:
    dispatcher = EventDispatcher()

    def skip(event: WorkerMessageReceivedEvent) -> None:
        _ = event.should_handle(value=False)

    dispatcher.add_listener(WorkerMessageReceivedEvent, skip)
    bus = RecordingBus()
    _ = bind_bus(broker, bus, event_dispatcher=dispatcher)
    body, headers = _body()

    handle = await _kick(broker, _INGEST, body, **{RETRIES_LABEL: 0, HEADERS_LABEL: headers})
    result = await handle.wait_result()

    assert not result.is_err
    assert bus.dispatched == []


@pytest.mark.parametrize(
    ("attempt", "will_retry"),
    [(0, True), (1, True), (2, False)],
)
async def test_a_failure_is_announced_then_raised_saying_whether_it_is_retried(
    broker: InMemoryBroker, attempt: int, will_retry: bool
) -> None:
    dispatcher = EventDispatcher()
    failures: list[WorkerMessageFailedEvent] = []
    dispatcher.add_listener(WorkerMessageFailedEvent, failures.append)
    _ = bind_bus(
        broker,
        RecordingBus(failure=RuntimeError("boom")),
        event_dispatcher=dispatcher,
        max_attempts=3,
    )
    body, headers = _body()

    handle = await _kick(broker, _INGEST, body, **{RETRIES_LABEL: attempt, HEADERS_LABEL: headers})
    result = await handle.wait_result()

    assert isinstance(result.error, RuntimeError)
    [failed] = failures
    assert failed.will_retry is will_retry
    assert failed.envelope.last(ErrorDetailsStamp) == ErrorDetailsStamp("RuntimeError", "boom")


async def test_without_max_attempts_no_failure_claims_a_retry(broker: InMemoryBroker) -> None:
    dispatcher = EventDispatcher()
    failures: list[WorkerMessageFailedEvent] = []
    dispatcher.add_listener(WorkerMessageFailedEvent, failures.append)
    _ = bind_bus(broker, RecordingBus(failure=RuntimeError("boom")), event_dispatcher=dispatcher)
    body, headers = _body()

    _ = await _kick(broker, _INGEST, body, **{RETRIES_LABEL: 0, HEADERS_LABEL: headers})

    assert failures[0].will_retry is False
