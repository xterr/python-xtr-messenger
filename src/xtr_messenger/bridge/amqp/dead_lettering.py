"""Publishing a delivery straight to the dead-letter queue.

One place writes a dead letter, however a message earned it: exhausted
retries go through
:class:`~xtr_messenger.bridge.amqp.dead_letter_middleware.DeadLetterMiddleware`,
while a poison delivery — unparsable, or naming a task nobody registered —
is sent here directly, because retrying it can never end differently.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Final, cast

from aio_pika import DeliveryMode, Message
from taskiq_aio_pika import AioPikaBroker

from xtr_messenger.exception import MessageBusError

if TYPE_CHECKING:
    from collections.abc import Mapping

    from aio_pika.abc import HeadersType
    from taskiq import AsyncBroker

__all__ = ["DETAIL_LIMIT", "publish_dead_letter"]

#: How many characters of an exception's message travel with a dead letter.
#: Enough to identify the failure without letting an unbounded message bloat
#: the header.
DETAIL_LIMIT: Final = 512


async def publish_dead_letter(
    broker: AsyncBroker,
    queue_name: str,
    body: bytes,
    headers: Mapping[str, object],
) -> None:
    """Publish ``body`` to the dead-letter queue, ``headers`` saying why.

    Raises:
        MessageBusError: If ``broker`` is not an AMQP broker, or its publish
            channel is not open — the caller's delivery then stays unsettled,
            so the broker redelivers it rather than losing it.
    """
    channel = broker.write_channel if isinstance(broker, AioPikaBroker) else None
    if channel is None:
        raise MessageBusError(
            f"cannot dead-letter to {queue_name!r}: the broker has no open publish channel"
        )
    _ = await channel.default_exchange.publish(
        Message(
            body=body,
            # The driver accepts any AMQP table value; this signature keeps
            # callers typed without re-declaring the driver's value union.
            headers=cast("HeadersType", dict(headers)),
            delivery_mode=DeliveryMode.PERSISTENT,
        ),
        routing_key=queue_name,
    )
