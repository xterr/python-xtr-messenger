from __future__ import annotations

from typing import TYPE_CHECKING, final
from uuid import uuid4

import msgspec
import pytest
from taskiq import InMemoryBroker, TaskiqMessage, TaskiqResult
from taskiq_aio_pika import AioPikaBroker

from message_bus.bridge.amqp.amqp_broker import create_amqp_broker
from message_bus.bridge.amqp.amqp_options import AmqpOptions
from message_bus.bridge.amqp.dead_letter_middleware import DeadLetterMiddleware
from message_bus.bridge.amqp.reliability import DEFAULT_DEAD_LETTER_QUEUE, Reliability
from message_bus.bridge.taskiq.labels import RETRIES_LABEL

if TYPE_CHECKING:
    from aio_pika.abc import AbstractMessage

pytestmark = pytest.mark.anyio

_MAX_ATTEMPTS = 3


@final
class FakeExchange:
    def __init__(self) -> None:
        self.published: list[tuple[AbstractMessage, str]] = []

    async def publish(self, message: AbstractMessage, routing_key: str) -> None:
        self.published.append((message, routing_key))


@final
class FakeChannel:
    def __init__(self) -> None:
        self.default_exchange = FakeExchange()


def a_message(retries: int) -> TaskiqMessage:
    return TaskiqMessage(
        task_id=str(uuid4()),
        task_name="test.ingest.v1",
        labels={RETRIES_LABEL: str(retries)},
        args=['{"document_id": "x"}'],
        kwargs={},
    )


def a_broker_with(channel: FakeChannel) -> AioPikaBroker:
    broker = AioPikaBroker(url="amqp://unused")
    setattr(broker, "write_channel", channel)  # noqa: B010
    return broker


def middleware_on(broker: AioPikaBroker) -> DeadLetterMiddleware:
    middleware = DeadLetterMiddleware(DEFAULT_DEAD_LETTER_QUEUE, _MAX_ATTEMPTS)
    middleware.set_broker(broker)
    return middleware


def a_failure() -> TaskiqResult[object]:
    return TaskiqResult[object](is_err=True, return_value=None, execution_time=0.0)


@pytest.mark.parametrize("retries", [0, 1])
async def test_a_message_with_attempts_left_is_not_dead_lettered(retries: int) -> None:
    channel = FakeChannel()
    middleware = middleware_on(a_broker_with(channel))

    await middleware.on_error(a_message(retries), a_failure(), RuntimeError("boom"))

    assert channel.default_exchange.published == []


async def test_the_final_attempt_republishes_to_the_dead_letter_queue() -> None:
    channel = FakeChannel()
    middleware = middleware_on(a_broker_with(channel))

    await middleware.on_error(a_message(_MAX_ATTEMPTS - 1), a_failure(), RuntimeError("boom"))

    assert len(channel.default_exchange.published) == 1
    _, routing_key = channel.default_exchange.published[0]
    assert routing_key == DEFAULT_DEAD_LETTER_QUEUE


async def test_the_dead_lettered_message_is_replayable() -> None:
    channel = FakeChannel()
    middleware = middleware_on(a_broker_with(channel))
    message = a_message(_MAX_ATTEMPTS - 1)

    await middleware.on_error(message, a_failure(), ValueError("bad payload"))

    published, _ = channel.default_exchange.published[0]
    replayed = msgspec.json.decode(published.body.decode(), type=dict[str, object])
    assert replayed["task_name"] == "test.ingest.v1"
    assert replayed["task_id"] == message.task_id
    assert replayed["args"] == ['{"document_id": "x"}']


async def test_the_dead_lettered_message_records_why_it_died() -> None:
    channel = FakeChannel()
    middleware = middleware_on(a_broker_with(channel))

    await middleware.on_error(a_message(_MAX_ATTEMPTS - 1), a_failure(), ValueError("bad payload"))

    published, _ = channel.default_exchange.published[0]
    assert published.headers["x-death-reason"] == "ValueError"
    assert published.headers["x-death-detail"] == "bad payload"


async def test_a_message_that_lost_its_retry_label_is_retried_not_dead_lettered() -> None:
    """Losing the label must not end the ladder on the first failure.

    taskiq reads the same label as ``labels.get("_retries", 0) + 1``, so an
    absent one is attempt 1 of the ladder and the message is redelivered.
    Dead-lettering here would move a message nobody had finished trying.
    """
    channel = FakeChannel()
    middleware = middleware_on(a_broker_with(channel))
    message = a_message(0)
    message.labels.clear()

    await middleware.on_error(message, a_failure(), RuntimeError("boom"))

    assert channel.default_exchange.published == []


async def test_a_broker_without_an_amqp_channel_is_left_alone() -> None:
    middleware = DeadLetterMiddleware(DEFAULT_DEAD_LETTER_QUEUE, _MAX_ATTEMPTS)
    middleware.set_broker(InMemoryBroker())

    await middleware.on_error(a_message(_MAX_ATTEMPTS - 1), a_failure(), RuntimeError("boom"))


def test_the_amqp_factory_wires_dead_lettering_by_default() -> None:
    broker = create_amqp_broker("amqp://guest:guest@localhost:5672/")

    assert any(isinstance(m, DeadLetterMiddleware) for m in broker.middlewares)


def test_dead_lettering_can_be_turned_off() -> None:
    broker = create_amqp_broker(
        "amqp://guest:guest@localhost:5672/",
        AmqpOptions(reliability=Reliability(dead_letter_queue=None)),
    )

    assert not any(isinstance(m, DeadLetterMiddleware) for m in broker.middlewares)


def test_building_a_broker_opens_no_connection() -> None:
    broker = create_amqp_broker("amqp://guest:guest@unreachable:5672/")

    assert broker.write_channel is None
