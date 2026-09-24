"""Registering a task per declared message, each dispatching into a bus."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pytest
from taskiq import InMemoryBroker

from message_bus import (
    Envelope,
    JsonSerializer,
    MessageDecodingFailedError,
    ReceivedStamp,
    RedeliveryStamp,
)
from message_bus.bridge.taskiq.binding import bind_bus
from message_bus.bridge.taskiq.broker import forget_started
from message_bus.bridge.taskiq.labels import HEADERS_LABEL, RETRIES_LABEL
from message_bus.message_registry import declared_names
from tests.support.fakes import RecordingBus
from tests.support.messages import ingest_document

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
