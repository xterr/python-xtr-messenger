"""The settings an AMQP transport reads from its configuration."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Final

from xtr_messenger.bridge.taskiq.taskiq_worker import default_max_async_tasks
from xtr_messenger.exception import InvalidTransportOptionError
from xtr_messenger.transport.transport_options import as_bool, as_int

from .connection_options import CONNECTION_OPTIONS, ConnectionOptions
from .exchange_options import EXCHANGE_OPTIONS, ExchangeOptions
from .queue_options import QUEUE_OPTIONS, QueueOptions
from .reliability import RELIABILITY_OPTIONS, Reliability

if TYPE_CHECKING:
    from collections.abc import Mapping

__all__ = ["AMQP_OPTIONS", "AmqpOptions"]

#: Every setting an ``amqp://`` transport accepts, from its DSN query string
#: or its ``options``. Anything else is rejected rather than ignored.
AMQP_OPTIONS: Final = (
    "prefetch_count",
    "max_async_tasks",
    "auto_setup",
    *RELIABILITY_OPTIONS,
    *EXCHANGE_OPTIONS,
    *QUEUE_OPTIONS,
    *CONNECTION_OPTIONS,
)

_DEFAULT_PREFETCH: Final = 10


@dataclass(frozen=True, slots=True)
class AmqpOptions:
    """What an ``amqp://`` transport was configured with.

    Grouped by what they configure. Credentials are absent on purpose: they
    travel in the DSN itself, where a URL already expresses them.

    Attributes:
        reliability: How hard to retry, and where exhausted messages go.
        exchange: The exchange messages are published through.
        queue: How each named queue is declared.
        connection: How the driver opens and keeps the connection.
        prefetch_count: How many messages a consumer may hold unacked. The
            lever for throughput against fair distribution across workers.
        max_async_tasks: How many messages a worker handles at once. Ten per
            CPU, capped at 100, unless configured. A message is acked once
            handled, so ``prefetch_count`` caps this too: a worker handles
            the smaller of the two at once.
        auto_setup: Whether to declare the exchange and queues on startup.
            Turn it off where a deployment creates its own topology and the
            application has no permission to.
    """

    reliability: Reliability = field(default_factory=Reliability)
    exchange: ExchangeOptions = field(default_factory=ExchangeOptions)
    queue: QueueOptions = field(default_factory=QueueOptions)
    connection: ConnectionOptions = field(default_factory=ConnectionOptions)
    prefetch_count: int = _DEFAULT_PREFETCH
    max_async_tasks: int = field(default_factory=default_max_async_tasks)
    auto_setup: bool = True

    @classmethod
    def from_settings(
        cls,
        settings: Mapping[str, str],
        defaults: AmqpOptions | None = None,
    ) -> AmqpOptions:
        """Read every option out of a transport's settings.

        Anything the settings do not mention keeps its value from
        ``defaults``, so a factory built with a policy in code can still have
        one setting overridden per transport.

        Raises:
            InvalidTransportOptionError: If a value is present but not usable.
        """
        base = defaults if defaults is not None else cls()
        return cls(
            reliability=Reliability.from_settings(settings, base.reliability),
            exchange=ExchangeOptions.from_settings(settings, base.exchange),
            queue=QueueOptions.from_settings(settings, base.queue),
            connection=ConnectionOptions.from_settings(settings, base.connection),
            prefetch_count=as_int(settings, "prefetch_count", base.prefetch_count),
            max_async_tasks=_max_async_tasks(settings, base.max_async_tasks),
            auto_setup=as_bool(settings, "auto_setup", base.auto_setup),
        )


def _max_async_tasks(settings: Mapping[str, str], fallback: int) -> int:
    """Read ``max_async_tasks``, refusing a limit that would mean no limit at all.

    Raises:
        InvalidTransportOptionError: If it is not a whole number above zero.
    """
    value = as_int(settings, "max_async_tasks", fallback)
    if value < 1:
        raise InvalidTransportOptionError(
            "max_async_tasks", settings["max_async_tasks"], "a whole number above zero"
        )
    return value
