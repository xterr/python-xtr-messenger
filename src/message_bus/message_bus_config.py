"""Describing the bus as settings.

Pure data: which transports exist, and which messages go to them. It builds
nothing — hand it to
:class:`~message_bus.message_bus_factory.MessageBusFactory` for that::

    CONFIG = MessageBusConfig(
        transports={
            "high": TransportConfig(AMQP_URL, queue="jobs_high"),
            "low": TransportConfig(AMQP_URL, queue="jobs_low"),
            "sync": TransportConfig("sync://"),
        },
        routing={
            UrgentJob: "high",
            AuditRecorded: ["low", "sync"],
            "*": "low",
        },
    )

Keeping it inert is what lets it come from anywhere — a settings module, an
environment variable, a parsed file — without dragging a broker along.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field

from .transport.transport_config import TransportConfig

__all__ = ["MessageBusConfig", "TransportConfig"]


def _no_routes() -> dict[type | str, str | Sequence[str]]:
    return {}


@dataclass(frozen=True, slots=True)
class MessageBusConfig:
    """The transports that exist, and which messages go to them.

    Attributes:
        transports: Named transports. A worker selects between these names,
            so one deployment can run a process per queue instead of one
            process consuming everything.
        routing: Message type (or ``"*"``) to transport name, or several
            names to fan out. A message may also name its own transport with
            :func:`~message_bus.decorator.as_message`; this map wins.
    """

    transports: Mapping[str, TransportConfig]
    routing: Mapping[type | str, str | Sequence[str]] = field(default_factory=_no_routes)

    def by_connection(self) -> dict[str, dict[str, TransportConfig]]:
        """Group the transports by the server each one addresses.

        Transports differing only in their query string share a server, and
        therefore share one connection — which is what lets an adapter open a
        single broker and hand it to every queue named on it.
        """
        grouped: dict[str, dict[str, TransportConfig]] = {}
        for name, transport in self.transports.items():
            grouped.setdefault(transport.parsed.connection, {})[name] = transport
        return grouped
