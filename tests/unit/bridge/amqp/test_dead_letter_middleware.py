"""Real dead-lettering for messages whose retries have run out.

taskiq's retry middleware acknowledges an exhausted message, so RabbitMQ
never dead-letters it. This middleware republishes it on the final failed
attempt instead, so exhausted work can be inspected and replayed.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, final
from uuid import uuid4

import msgspec
import pytest
from aio_pika import Connection, Exchange, Message
from aio_pika.channel import Channel
from taskiq import InMemoryBroker, TaskiqMessage, TaskiqResult
from taskiq_aio_pika import AioPikaBroker
from typing_extensions import override
from yarl import URL

from message_bus.bridge.amqp.dead_letter_middleware import DeadLetterMiddleware
from message_bus.bridge.amqp.reliability import DEFAULT_DEAD_LETTER_QUEUE
from message_bus.bridge.taskiq.labels import RETRIES_LABEL

if TYPE_CHECKING:
    from aio_pika.abc import AbstractChannel, AbstractMessage, TimeoutType

pytestmark = pytest.mark.anyio

_MAX_ATTEMPTS = 3


@final
class RecordingExchange(Exchange):
    """A default exchange that records what was published to it.

    Subclasses the real ``Exchange`` — so it is a genuine ``AbstractExchange``
    the channel can hold — and overrides only ``publish``. Its superclass is
    constructed against a real but unconnected channel; nothing opens I/O.
    """

    def __init__(self, channel: AbstractChannel) -> None:
        super().__init__(channel, name="")
        self.published: list[tuple[AbstractMessage, str]] = []

    @override
    async def publish(
        self,
        message: AbstractMessage,
        routing_key: str,
        *,
        mandatory: bool = True,
        immediate: bool = False,
        timeout: TimeoutType = None,
    ) -> None:
        del mandatory, immediate, timeout
        self.published.append((message, routing_key))


def _broker_with_recording_exchange() -> tuple[AioPikaBroker, RecordingExchange]:
    """Give an AMQP broker a write channel whose default exchange records.

    The channel and exchange are real aio-pika objects built offline: no
    connection is opened, and only the exchange's ``publish`` is replaced.
    """
    broker = AioPikaBroker(url="amqp://unused")
    channel = Channel(Connection(URL("amqp://unused")))
    exchange = RecordingExchange(channel)
    channel.default_exchange = exchange
    broker.write_channel = channel
    return broker, exchange


def _message(retries: int) -> TaskiqMessage:
    return TaskiqMessage(
        task_id=str(uuid4()),
        task_name="test.ingest.v1",
        labels={RETRIES_LABEL: str(retries)},
        args=['{"document_id": "x"}'],
        kwargs={},
    )


def _failure() -> TaskiqResult[object]:
    return TaskiqResult(is_err=True, return_value=None, execution_time=0.0)


def _middleware_on(broker: AioPikaBroker) -> DeadLetterMiddleware:
    middleware = DeadLetterMiddleware(DEFAULT_DEAD_LETTER_QUEUE, _MAX_ATTEMPTS)
    middleware.set_broker(broker)
    return middleware


@pytest.mark.parametrize("retries", [0, 1])
async def test_a_message_with_attempts_left_is_not_dead_lettered(retries: int) -> None:
    broker, exchange = _broker_with_recording_exchange()
    middleware = _middleware_on(broker)

    await middleware.on_error(_message(retries), _failure(), RuntimeError("boom"))

    assert exchange.published == []


async def test_the_final_attempt_republishes_to_the_dead_letter_queue() -> None:
    broker, exchange = _broker_with_recording_exchange()
    middleware = _middleware_on(broker)

    await middleware.on_error(_message(_MAX_ATTEMPTS - 1), _failure(), RuntimeError("boom"))

    assert len(exchange.published) == 1
    _, routing_key = exchange.published[0]
    assert routing_key == DEFAULT_DEAD_LETTER_QUEUE


async def test_the_dead_lettered_message_is_replayable() -> None:
    broker, exchange = _broker_with_recording_exchange()
    middleware = _middleware_on(broker)
    message = _message(_MAX_ATTEMPTS - 1)

    await middleware.on_error(message, _failure(), ValueError("bad payload"))

    published, _ = exchange.published[0]
    assert isinstance(published, Message)
    replayed = msgspec.json.decode(published.body, type=dict[str, object])
    assert replayed["task_name"] == "test.ingest.v1"
    assert replayed["task_id"] == message.task_id
    assert replayed["args"] == ['{"document_id": "x"}']


async def test_the_dead_lettered_message_records_why_it_died() -> None:
    broker, exchange = _broker_with_recording_exchange()
    middleware = _middleware_on(broker)

    await middleware.on_error(_message(_MAX_ATTEMPTS - 1), _failure(), ValueError("bad payload"))

    published, _ = exchange.published[0]
    assert isinstance(published, Message)
    assert published.headers["x-death-reason"] == "ValueError"
    assert published.headers["x-death-detail"] == "bad payload"


async def test_a_message_that_lost_its_retry_label_is_retried_not_dead_lettered() -> None:
    """Losing the label must not end the ladder on the first failure.

    taskiq reads an absent label as attempt 1 of the ladder and redelivers,
    so dead-lettering here would move a message nobody had finished trying.
    """
    broker, exchange = _broker_with_recording_exchange()
    middleware = _middleware_on(broker)
    message = _message(0)
    message.labels.clear()

    await middleware.on_error(message, _failure(), RuntimeError("boom"))

    assert exchange.published == []


async def test_an_unreadable_retry_count_is_not_treated_as_final() -> None:
    """A count that will not parse is read as "not final": erring towards
    another delivery is recoverable, dead-lettering is not."""
    broker, exchange = _broker_with_recording_exchange()
    middleware = _middleware_on(broker)
    message = _message(0)
    message.labels[RETRIES_LABEL] = "later"

    await middleware.on_error(message, _failure(), RuntimeError("boom"))

    assert exchange.published == []


async def test_a_non_amqp_broker_is_left_alone() -> None:
    middleware = DeadLetterMiddleware(DEFAULT_DEAD_LETTER_QUEUE, _MAX_ATTEMPTS)
    middleware.set_broker(InMemoryBroker())

    await middleware.on_error(_message(_MAX_ATTEMPTS - 1), _failure(), RuntimeError("boom"))


async def test_a_broker_with_no_write_channel_is_left_alone() -> None:
    """Construction opens no connection, so ``write_channel`` is ``None`` and
    there is nowhere to republish; the middleware simply returns."""
    middleware = _middleware_on(AioPikaBroker(url="amqp://unused"))

    await middleware.on_error(_message(_MAX_ATTEMPTS - 1), _failure(), RuntimeError("boom"))
