"""Building a RabbitMQ broker with retries, delays and dead-lettering wired up.

Use this factory rather than constructing ``AioPikaBroker`` directly: it is
where the retry ladder, the delayed-message exchange, real dead-lettering
and single OpenTelemetry instrumentation are wired. A hand-built broker gets
none of them.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Final

from taskiq import SmartRetryMiddleware
from taskiq_aio_pika import AioPikaBroker
from taskiq_aio_pika.queue import Queue

from .amqp_options import AmqpOptions
from .dead_letter_middleware import DeadLetterMiddleware

if TYPE_CHECKING:
    from collections.abc import Sequence

__all__ = ["create_amqp_broker", "declared_queues"]

_MAX_DELAY_SECONDS: Final = 60.0


def create_amqp_broker(
    url: str,
    options: AmqpOptions | None = None,
    queues: Sequence[str] = (),
) -> AioPikaBroker:
    """Build a RabbitMQ broker.

    Construction opens no connection, so this is safe to call at import time
    — which is what a module-level broker in a worker entrypoint needs.

    Pass ``queues`` to publish to more than one queue; a
    :class:`~message_bus.bridge.taskiq.taskiq_sender.TaskiqSender` then
    selects one by name, which is how a routing table maps message types onto
    queues.
    """
    configured = options if options is not None else AmqpOptions()
    settings = configured.reliability
    broker = AioPikaBroker(
        url=configured.connection.applied_to(url),
        qos=configured.prefetch_count,
        exchange=configured.exchange.declared(configured.auto_setup),
        task_queues=[configured.queue.declared(name, configured.auto_setup) for name in queues]
        or None,
        dead_letter_queue=(
            Queue(name=settings.dead_letter_queue)
            if settings.dead_letter_queue is not None
            else None
        ),
        delayed_message_exchange_plugin=True,
    ).with_middlewares(
        SmartRetryMiddleware(
            default_retry_count=settings.max_attempts,
            default_delay=settings.base_delay_seconds,
            use_delay_exponent=True,
            max_delay_exponent=_MAX_DELAY_SECONDS,
            default_retry_label=True,
        ),
    )
    if settings.dead_letter_queue is not None:
        return broker.with_middlewares(
            DeadLetterMiddleware(settings.dead_letter_queue, settings.max_attempts),
        )
    return broker


def declared_queues(broker: AioPikaBroker) -> tuple[str, ...]:
    """Return the queues ``broker`` declares, which are the ones it consumes.

    Useful at worker startup to log the workload a process has taken on, and
    to confirm a deployment scoped its workers the way it intended.
    """
    return tuple(queue.name for queue in broker._task_queues)  # noqa: SLF001
