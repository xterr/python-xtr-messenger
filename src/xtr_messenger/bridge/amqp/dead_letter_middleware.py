"""Real dead-lettering for exhausted messages.

taskiq's retry middleware acknowledges a message once its attempts run out,
which means RabbitMQ never dead-letters it: the message is simply gone, and
the only trace is a log line. This middleware republishes the message to the
dead-letter queue on the final failed attempt, so exhausted work can be
inspected and replayed instead of disappearing.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, final

from aio_pika import DeliveryMode, Message
from taskiq import TaskiqMiddleware
from taskiq_aio_pika import AioPikaBroker
from typing_extensions import override

from xtr_messenger.bridge.taskiq.labels import RETRIES_LABEL

if TYPE_CHECKING:
    from taskiq import TaskiqMessage, TaskiqResult

__all__ = ["DeadLetterMiddleware"]


@final
class DeadLetterMiddleware(TaskiqMiddleware):
    """Republishes a message to the dead-letter queue once retries run out."""

    queue_name: str
    max_attempts: int

    def __init__(self, queue_name: str, max_attempts: int) -> None:
        """Dead-letter to ``queue_name`` after ``max_attempts`` deliveries."""
        super().__init__()
        self.queue_name = queue_name
        self.max_attempts = max_attempts

    @override
    async def on_error(
        self,
        message: TaskiqMessage,
        result: TaskiqResult[object],
        exception: BaseException,
    ) -> None:
        """Dead-letter the message if this was its last attempt."""
        del result
        broker = self.broker
        if not isinstance(broker, AioPikaBroker) or not self._is_final_attempt(message):
            return
        channel = broker.write_channel
        if channel is None:
            return
        _ = await channel.default_exchange.publish(
            self._dead_letter(message, exception),
            routing_key=self.queue_name,
        )

    def _is_final_attempt(self, message: TaskiqMessage) -> bool:
        """Report whether this delivery was the last one the ladder allows.

        An unreadable count means *not* final. The sender always sets the
        label, so its absence says the message reached the queue some other
        way — and reading that as "attempts exhausted" would dead-letter it
        on its first failure, turning one unknown into a message nobody
        retries. Erring towards another delivery is recoverable; erring
        towards the dead-letter queue is not.

        This is the opposite of the reading
        :func:`~xtr_messenger.bridge.taskiq.binding.bind_bus` gives a
        handler, and deliberately so: there an unknown count is reported high
        so a handler treats the delivery as its last chance and does not skip
        cleanup. Both choices pick the outcome that loses nothing.
        """
        raw = message.labels.get(RETRIES_LABEL)
        if isinstance(raw, bool) or not isinstance(raw, (int, str)):
            return False
        try:
            return int(raw) >= self.max_attempts - 1
        except ValueError:
            return False

    def _dead_letter(self, message: TaskiqMessage, exception: BaseException) -> Message:
        broker_message = self.broker.formatter.dumps(message)
        return Message(
            body=broker_message.message,
            headers={
                "task_id": message.task_id,
                "task_name": message.task_name,
                "x-death-reason": type(exception).__name__,
                "x-death-detail": str(exception)[:_DETAIL_LIMIT],
                **message.labels,
            },
            delivery_mode=DeliveryMode.PERSISTENT,
        )


_DETAIL_LIMIT = 512
