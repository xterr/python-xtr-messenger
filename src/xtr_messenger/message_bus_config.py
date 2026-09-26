"""Describing the bus as settings.

Pure data: which transports exist, which messages go to them, and what the
dispatch chain is made of. It builds nothing — hand it to
:class:`~xtr_messenger.message_bus_factory.MessageBusFactory` for that::

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
        middleware=["logging"],
    )

Keeping it inert is what lets it come from anywhere — a settings module, an
environment variable, a parsed file — without dragging a broker along. Naming
middleware rather than building it is what keeps that true of the chain too.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field

from .middleware.middleware_interface import MiddlewareInterface
from .transport.transport_config import TransportConfig

__all__ = ["MessageBusConfig", "TransportConfig"]


def _no_routes() -> dict[type | str, str | Sequence[str]]:
    return {}


def _no_transports() -> dict[str, TransportConfig]:
    return {}


@dataclass(frozen=True, slots=True)
class MessageBusConfig:
    """The transports that exist, which messages go to them, and the chain.

    Attributes:
        transports: Named transports. A worker selects between these names,
            so one deployment can run a process per queue instead of one
            process consuming everything.
        routing: Message type (or ``"*"``) to transport name, or several
            names to fan out. A message may also name its own transport with
            :func:`~xtr_messenger.decorator.as_message`; this map wins.
        middleware: The middleware to run, in order, ahead of routing and
            handling: a name, or middleware already built. ``"logging"`` is
            the name this library ships; a factory's ``named`` adds more. An
            unknown one is refused with
            :class:`~xtr_messenger.exception.UnknownMiddlewareError`.
            Middleware keeps no per-message state: one instance serves every
            dispatch, concurrently, and may serve the bus and the workers
            alike.
        default_middleware: Whether routing and handling are appended at all,
            on the bus and in every worker. ``False`` runs only what
            ``middleware`` names, for a chain composed entirely by hand.
        require_sender: Refuse a message no transport is routed for, rather
            than letting the dispatch pass quietly.
        handle_unrouted: Handle a message routed nowhere in this process,
            rather than letting the dispatch pass quietly.
    """

    transports: Mapping[str, TransportConfig] = field(default_factory=_no_transports)
    routing: Mapping[type | str, str | Sequence[str]] = field(default_factory=_no_routes)
    middleware: Sequence[str | MiddlewareInterface] = ()
    default_middleware: bool = True
    require_sender: bool = False
    handle_unrouted: bool = False

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
