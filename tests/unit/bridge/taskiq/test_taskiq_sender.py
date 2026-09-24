"""Publishing an envelope as a taskiq task, addressed by the message's name."""

from __future__ import annotations

from typing import TYPE_CHECKING

import msgspec
import pytest
from taskiq import InMemoryBroker

from tests.support.messages import ingest_document
from tests.support.taskiq import RecordingMiddleware
from xtr_messenger import DelayStamp, Envelope, JsonSerializer, TransportMessageIdStamp
from xtr_messenger.bridge.taskiq.broker import forget_started
from xtr_messenger.bridge.taskiq.labels import HEADERS_LABEL, QUEUE_LABEL, RETRIES_LABEL
from xtr_messenger.bridge.taskiq.taskiq_sender import TaskiqSender

if TYPE_CHECKING:
    from collections.abc import Iterator

pytestmark = pytest.mark.anyio

_INGEST = "test.ingest.v1"


@pytest.fixture
def broker() -> Iterator[InMemoryBroker]:
    made = InMemoryBroker(await_inplace=True)
    yield made
    forget_started(made)


def _sink(broker: InMemoryBroker, bodies: list[str]) -> None:
    """Register a task under the message's name so a publish has a home.

    A publish to a name no task serves raises ``UnknownTaskError`` before it
    reaches the broker, so the message's own name must resolve to something.
    """

    async def sink(body: str) -> None:
        bodies.append(body)

    _ = broker.register_task(sink, task_name=_INGEST)


def _captured(broker: InMemoryBroker) -> list[dict[str, object]]:
    labels: list[dict[str, object]] = []
    _ = broker.add_middlewares(RecordingMiddleware(labels))
    return labels


async def test_the_task_name_is_the_message_name(broker: InMemoryBroker) -> None:
    """The message name doubles as the task name, so a task registered under
    that name is the one a publish reaches."""
    bodies: list[str] = []
    _sink(broker, bodies)
    wire = JsonSerializer()
    message = ingest_document()

    _ = await TaskiqSender(broker, serializer=wire).send(Envelope.wrap(message))

    assert bodies == [wire.encode(Envelope.wrap(message)).body]


async def test_every_publish_marks_the_first_delivery(broker: InMemoryBroker) -> None:
    """A missing ``_retries`` is anomalous only because it is always set to 0."""
    _sink(broker, [])
    labels = _captured(broker)

    _ = await TaskiqSender(broker).send(Envelope.wrap(ingest_document()))

    assert int(str(labels[0][RETRIES_LABEL])) == 0


async def test_the_headers_label_carries_the_encoded_headers(broker: InMemoryBroker) -> None:
    _sink(broker, [])
    labels = _captured(broker)

    _ = await TaskiqSender(broker).send(Envelope.wrap(ingest_document()))

    raw = labels[0][HEADERS_LABEL]
    assert isinstance(raw, str)
    assert msgspec.json.decode(raw, type=dict[str, str])["type"] == _INGEST


async def test_a_pinned_queue_travels_as_a_label(broker: InMemoryBroker) -> None:
    _sink(broker, [])
    labels = _captured(broker)

    _ = await TaskiqSender(broker, queue="jobs_high").send(Envelope.wrap(ingest_document()))

    assert labels[0][QUEUE_LABEL] == "jobs_high"


async def test_no_queue_label_when_none_is_pinned(broker: InMemoryBroker) -> None:
    _sink(broker, [])
    labels = _captured(broker)

    _ = await TaskiqSender(broker).send(Envelope.wrap(ingest_document()))

    assert QUEUE_LABEL not in labels[0]


async def test_a_delay_stamp_becomes_a_delay_in_seconds(broker: InMemoryBroker) -> None:
    _sink(broker, [])
    labels = _captured(broker)
    envelope = Envelope.wrap(ingest_document()).with_stamps(DelayStamp(5000))

    _ = await TaskiqSender(broker).send(envelope)

    assert float(str(labels[0]["delay"])) == 5.0


async def test_send_reports_the_transport_message_id(broker: InMemoryBroker) -> None:
    _sink(broker, [])

    result = await TaskiqSender(broker).send(Envelope.wrap(ingest_document()))

    stamp = result.last(TransportMessageIdStamp)
    assert stamp is not None
    assert stamp.message_id


def test_it_exposes_its_broker_queue_and_serializer(broker: InMemoryBroker) -> None:
    wire = JsonSerializer()

    sender = TaskiqSender(broker, serializer=wire, queue="jobs")

    assert sender.broker is broker
    assert sender.queue == "jobs"
    assert sender.serializer is wire


def test_it_defaults_to_no_queue_and_a_json_serializer(broker: InMemoryBroker) -> None:
    sender = TaskiqSender(broker)

    assert sender.queue is None
    assert isinstance(sender.serializer, JsonSerializer)
