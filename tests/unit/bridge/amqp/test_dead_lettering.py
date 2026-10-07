"""Publishing a delivery straight to the dead-letter queue."""

from __future__ import annotations

from typing import TYPE_CHECKING, cast, final

import pytest
from taskiq import InMemoryBroker
from taskiq_aio_pika import AioPikaBroker

from xtr_messenger.bridge.amqp.dead_lettering import publish_dead_letter
from xtr_messenger.exception import MessageBusError

if TYPE_CHECKING:
    from aio_pika.abc import AbstractChannel

pytestmark = pytest.mark.anyio


@final
class FakeExchange:
    """Records what is published through the default exchange."""

    def __init__(self) -> None:
        self.published: list[tuple[object, str]] = []

    async def publish(self, message: object, routing_key: str) -> None:
        self.published.append((message, routing_key))


@final
class FakeChannel:
    """The slice of an open channel dead-lettering touches."""

    def __init__(self) -> None:
        self.default_exchange: FakeExchange = FakeExchange()


async def test_it_publishes_the_body_to_the_queue_with_its_headers() -> None:
    broker = AioPikaBroker()
    channel = FakeChannel()
    broker.write_channel = cast("AbstractChannel", cast("object", channel))

    await publish_dead_letter(broker, "jobs.dlq", b"body", {"x-death-reason": "poison"})

    [(message, routing_key)] = channel.default_exchange.published
    assert routing_key == "jobs.dlq"
    body = getattr(message, "body", None)
    headers = getattr(message, "headers", None)
    assert body == b"body"
    assert headers is not None
    assert headers["x-death-reason"] == "poison"


async def test_a_broker_without_an_open_channel_fails_loudly() -> None:
    """The delivery then stays with the broker rather than being dropped."""
    with pytest.raises(MessageBusError, match=r"jobs\.dlq"):
        await publish_dead_letter(AioPikaBroker(), "jobs.dlq", b"body", {})


async def test_a_broker_that_is_not_amqp_fails_loudly() -> None:
    with pytest.raises(MessageBusError):
        await publish_dead_letter(InMemoryBroker(), "jobs.dlq", b"body", {})
