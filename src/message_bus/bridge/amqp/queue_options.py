"""How the queues a transport reads are declared."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Final

from taskiq_aio_pika.queue import Queue, QueueType

from message_bus.transport.transport_options import as_bool, as_choice, as_optional_int

if TYPE_CHECKING:
    from collections.abc import Mapping

__all__ = ["QUEUE_OPTIONS", "QueueOptions"]

#: Settings :meth:`QueueOptions.from_settings` reads.
QUEUE_OPTIONS: Final = (
    "queue",
    "queue_type",
    "queue_durable",
    "queue_auto_delete",
    "queue_exclusive",
    "queue_max_priority",
    "routing_key",
)

_QUEUE_TYPES: Final = tuple(member.value for member in QueueType)


@dataclass(frozen=True, slots=True)
class QueueOptions:
    """How each queue this transport names is declared.

    One set applies to every queue on the connection: transports sharing a
    DSN share a broker and differ only in which queue they name, so there is
    nothing per-queue left to vary.

    Attributes:
        type: ``quorum`` replicates across nodes and is taskiq's default;
            ``classic`` is the older single-node queue; ``stream`` is an
            append-only log.
        durable: Whether the queue survives a broker restart.
        auto_delete: Whether the broker removes it once the last consumer
            disconnects.
        exclusive: Whether the queue belongs to one connection only.
        max_priority: Enables a priority queue up to this value.
        routing_key: The binding key, when it differs from the queue name.
    """

    type: str = QueueType.QUORUM.value
    durable: bool = True
    auto_delete: bool = False
    exclusive: bool = False
    max_priority: int | None = None
    routing_key: str | None = None

    @classmethod
    def from_settings(
        cls,
        settings: Mapping[str, str],
        defaults: QueueOptions | None = None,
    ) -> QueueOptions:
        """Read the queue settings, falling back to ``defaults``.

        Raises:
            InvalidDsnError: If a value is present but not usable.
        """
        base = defaults if defaults is not None else cls()
        return cls(
            type=as_choice(settings, "queue_type", _QUEUE_TYPES, base.type),
            durable=as_bool(settings, "queue_durable", base.durable),
            auto_delete=as_bool(settings, "queue_auto_delete", base.auto_delete),
            exclusive=as_bool(settings, "queue_exclusive", base.exclusive),
            max_priority=as_optional_int(settings, "queue_max_priority", base.max_priority),
            routing_key=settings.get("routing_key", base.routing_key),
        )

    def declared(self, name: str, declare: bool) -> Queue:
        """Return the queue named ``name``, declared as configured."""
        return Queue(
            name=name,
            type=QueueType(self.type),
            durable=self.durable,
            auto_delete=self.auto_delete,
            exclusive=self.exclusive,
            max_priority=self.max_priority,
            routing_key=self.routing_key,
            declare=declare,
        )
