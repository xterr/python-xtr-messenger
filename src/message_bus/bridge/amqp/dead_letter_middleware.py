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

from message_bus.bridge.taskiq.labels import RETRIES_LABEL

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
        raw = message.labels.get(RETRIES_LABEL)
        if isinstance(raw, bool) or not isinstance(raw, (int, str)):
            return True
        try:
            return int(raw) >= self.max_attempts - 1
        except ValueError:
            return True

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
