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

from .exception import UnknownTransportError
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
            handling: a name, middleware already built, or a name with
            arguments for its constructor — ``{"audit": {"channel":
            "billing"}}``. ``"logging"`` is the name this library ships; a
            factory's ``named`` adds more. An unknown one is refused with
            :class:`~xtr_messenger.exception.UnknownMiddlewareError`, and
            arguments it does not take with
            :class:`~xtr_messenger.exception.InvalidMiddlewareArgumentsError`.
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
        require_handler: Refuse a message handled here that no handler
            takes. ``False`` for a bus shared by processes that each handle
            some of its messages.
        failure_transport: The transport a message is sent to once its last
            attempt failed, for every transport that names none of its own —
            and the one the ``messenger:failed:*`` commands read when told no
            other. See :attr:`TransportConfig.failure_transport`.
    """

    transports: Mapping[str, TransportConfig] = field(default_factory=_no_transports)
    routing: Mapping[type | str, str | Sequence[str]] = field(default_factory=_no_routes)
    middleware: Sequence[str | MiddlewareInterface | Mapping[str, Mapping[str, object]]] = ()
    default_middleware: bool = True
    require_sender: bool = False
    handle_unrouted: bool = False
    require_handler: bool = True
    failure_transport: str | None = None

    def __post_init__(self) -> None:
        """Refuse a failure transport that is not a configured transport.

        Raises:
            UnknownTransportError: If ``failure_transport``, or a transport's
                own, names a transport the configuration does not define.
        """
        missing = tuple(n for n in self.failure_transport_names() if n not in self.transports)
        if missing:
            raise UnknownTransportError(missing, tuple(self.transports))

    def failure_transport_names(self) -> tuple[str, ...]:
        """Return every failure transport: the configuration's own first, then each transport's."""
        named = (
            self.failure_transport,
            *(transport.failure_transport for transport in self.transports.values()),
        )
        return tuple(dict.fromkeys(name for name in named if name is not None))

    def failure_transport_of(self, name: str) -> str | None:
        """Return where the transport ``name`` keeps what failed for good, if anywhere.

        ``None`` too for a transport that is its own failure transport: what
        fails there for good has nowhere left to go. And ``None`` for a name
        the configuration does not define — a receiver registered on its own,
        as the scheduler's are: only a configured transport has a failure
        transport, its own or the configuration's.
        """
        transport = self.transports.get(name)
        if transport is None:
            return None
        own = transport.failure_transport
        failure = own if own is not None else self.failure_transport
        return None if failure == name else failure

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
