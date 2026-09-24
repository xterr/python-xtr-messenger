"""How the exchange a transport publishes through is declared."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Final

from aio_pika.abc import ExchangeType
from taskiq_aio_pika.exchange import Exchange

from xtr_messenger.transport.transport_options import as_bool, as_choice

if TYPE_CHECKING:
    from collections.abc import Mapping

__all__ = ["EXCHANGE_OPTIONS", "ExchangeOptions"]

#: Settings :meth:`ExchangeOptions.from_settings` reads.
EXCHANGE_OPTIONS: Final = (
    "exchange",
    "exchange_type",
    "exchange_durable",
    "exchange_auto_delete",
)

_EXCHANGE_TYPES: Final = tuple(member.value for member in ExchangeType)


@dataclass(frozen=True, slots=True)
class ExchangeOptions:
    """The exchange messages are published through.

    Flat and prefixed rather than nested, because a DSN query string has no
    nesting and a setting must read the same in either place it can be
    written.

    Attributes:
        name: The exchange name, or ``None`` for taskiq's default.
        type: One of RabbitMQ's exchange types. ``topic`` routes on patterns
            and is what taskiq assumes.
        durable: Whether the exchange survives a broker restart.
        auto_delete: Whether the broker removes it once the last queue
            unbinds.
    """

    name: str | None = None
    type: str = ExchangeType.TOPIC.value
    durable: bool = True
    auto_delete: bool = False

    @classmethod
    def from_settings(
        cls,
        settings: Mapping[str, str],
        defaults: ExchangeOptions | None = None,
    ) -> ExchangeOptions:
        """Read the exchange settings, falling back to ``defaults``.

        Raises:
            InvalidTransportOptionError: If a value is present but not usable.
        """
        base = defaults if defaults is not None else cls()
        return cls(
            name=settings.get("exchange", base.name),
            type=as_choice(settings, "exchange_type", _EXCHANGE_TYPES, base.type),
            durable=as_bool(settings, "exchange_durable", base.durable),
            auto_delete=as_bool(settings, "exchange_auto_delete", base.auto_delete),
        )

    def declared(self, declare: bool) -> Exchange | None:
        """Return the exchange to build, or ``None`` to accept taskiq's."""
        if self.name is None:
            return None
        return Exchange(
            name=self.name,
            type=ExchangeType(self.type),
            durable=self.durable,
            auto_delete=self.auto_delete,
            declare=declare,
        )
