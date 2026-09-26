"""Building what a worker process runs."""

from __future__ import annotations

from typing import TYPE_CHECKING, final

from .exception import IncompatibleReceiversError, NotConsumableError, UnknownTransportError
from .handler import RedispatchingHandlers, default_registry
from .message_bus import MessageBus
from .message_bus_factory import MessageBusFactory
from .middleware import HandleMessageMiddleware
from .middleware.named import chain, named_middleware
from .transport.receiver.chained_receiver import ChainedReceiver
from .transport.receiver.receiver_interface import ReceiverInterface
from .transport.transport_factory import TransportFactory
from .worker import Worker
from .worker_providing_interface import WorkerProvidingInterface

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from xtr_event_dispatcher_contracts import EventDispatcherInterface
    from xtr_logging_contracts import LoggerInterface

    from .handler import HandlersLocatorInterface
    from .message_bus_config import MessageBusConfig
    from .message_bus_interface import MessageBusInterface
    from .middleware.named import MiddlewareBuilder
    from .transport.sender import SenderInterface
    from .transport.transport_config import TransportConfig
    from .transport.transport_factory_interface import TransportFactoryInterface
    from .worker import AsyncResetter
    from .worker_interface import WorkerInterface

__all__ = ["WorkerFactory"]


@final
class WorkerFactory:
    """Builds what a worker process runs.

    One worker serves the transports it names and nothing else, so a
    deployment can run a process per queue. The result is ready to run: the
    handlers declared with
    :func:`~xtr_messenger.decorator.as_message_handler` are already
    reachable from the bus it dispatches through.

    What comes back is a :class:`~xtr_messenger.worker_interface.WorkerInterface`, never
    the broker underneath. That is what keeps the broker replaceable: an
    entrypoint says ``await worker.run()`` and never learns whether it got
    the library's own receive loop or one a broker library brought with it.
    """

    __slots__ = (
        "_bus",
        "_config",
        "_event_dispatcher",
        "_handlers",
        "_named",
        "_publishing",
        "_receivers",
        "_resetter",
        "_transports",
    )

    def __init__(  # noqa: PLR0913 — everything past `bus` is keyword-only
        self,
        config: MessageBusConfig,
        factories: Sequence[TransportFactoryInterface] | None = None,
        handlers: HandlersLocatorInterface | None = None,
        bus: MessageBusInterface | None = None,
        *,
        logger: LoggerInterface | None = None,
        named: Mapping[str, MiddlewareBuilder] | None = None,
        resetter: AsyncResetter | None = None,
        event_dispatcher: EventDispatcherInterface | None = None,
        receivers: Mapping[str, ReceiverInterface] | None = None,
    ) -> None:
        """Build from ``config``; ``bus`` overrides the one built for handling.

        ``handlers`` replaces the process-wide registry in the bus this
        worker dispatches through, which is the only place a handler is
        called — an adapter bringing its own worker, as the AMQP one does,
        dispatches into that bus too.

        ``logger`` and ``named`` mean what they do for
        :class:`~xtr_messenger.message_bus_factory.MessageBusFactory`: what
        the ``"logging"`` middleware writes through, and names of your own.

        ``event_dispatcher`` is handed to every worker built, which announce
        themselves and each message through it — see :mod:`xtr_messenger.event`.

        ``receivers`` are consumable by name beside the configured transports:
        something that only receives — messages a process generates, say —
        and so has no DSN to configure. A configured transport of the same
        name wins.
        """
        self._config = config
        self._transports = TransportFactory(factories)
        self._handlers = handlers
        self._bus = bus
        self._named = named_middleware(logger, named)
        self._resetter = resetter
        self._event_dispatcher = event_dispatcher
        self._publishing: MessageBusInterface | None = None
        self._receivers: dict[str, ReceiverInterface] = dict(receivers or {})

    def worker(self, names: Sequence[str]) -> WorkerInterface:
        """Build the worker for exactly the named transports.

        Import the modules that declare your handlers first — a handler that
        has not been declared cannot be found.

        A name is a configured transport or a registered receiver; a worker
        may consume both kinds at once, unless a configured transport brings
        its own worker.

        Raises:
            UnknownTransportError: If a name is neither.
            UnsupportedDsnError: If no factory recognises their DSN.
            IncompatibleReceiversError: If a transport bringing its own worker
                is named together with a registered receiver.
        """
        group = self._select(names)
        registered = {name: self._receivers[name] for name in names if name not in group}
        bus = self._dispatcher()
        found: dict[str, ReceiverInterface] = {}
        if group:
            factory = self._transports.serving(group)
            if isinstance(factory, WorkerProvidingInterface):
                if registered:
                    raise IncompatibleReceiversError(tuple(group), tuple(registered))
                return factory.worker(group, bus, event_dispatcher=self._event_dispatcher)
            found = _receivers_of(factory.create(group))
        found.update(registered)
        ordered = [name for name in names if name in found]
        return Worker(
            bus,
            _one_receiver([found[name] for name in ordered], ordered),
            self._resetter,
            event_dispatcher=self._event_dispatcher,
            receiver_name=ordered[0] if len(ordered) == 1 else None,
        )

    def _dispatcher(self) -> MessageBusInterface:
        """Return the bus a collected message is dispatched through.

        Handling only. An envelope that arrived from a transport carries a
        :class:`~xtr_messenger.stamp.ReceivedStamp` and is deliberately never
        routed again, so a worker's bus would build every sender in the
        configuration and then never use one — on AMQP that is a second
        connection per worker, opened and idle.

        What the configuration names still runs, so a consuming process
        reports the same way a publishing one does.

        A handler that publishes does so through the bus it was given, which
        is the producing one built by
        :class:`~xtr_messenger.message_bus_factory.MessageBusFactory`. Pass
        ``bus`` to supply that here instead, composed already — handling a
        :class:`~xtr_messenger.message.RedispatchMessage` is then that
        bus's business too.

        Otherwise a :class:`~xtr_messenger.message.RedispatchMessage` with
        no handler of its own is dispatched again through a publishing bus
        built from this factory's configuration, on first use.

        Raises:
            UnknownMiddlewareError: If the configuration names middleware
                ``named`` has nothing registered for.
        """
        if self._bus is not None:
            return self._bus
        handlers = self._handlers if self._handlers is not None else default_registry()
        handling = RedispatchingHandlers(handlers, self._publishing_bus)
        return MessageBus(
            chain(self._config, self._named, lambda: [HandleMessageMiddleware(handling)])
        )

    def _publishing_bus(self) -> MessageBusInterface:
        """Return the bus a redispatch goes out through, built on the first one.

        A redispatch must be routed, which the worker's own bus never does.
        Built from the same configuration, transports and handlers, so it
        routes exactly as the application's publishing bus does — and only
        when needed, since it opens every sender's connection.
        """
        if self._publishing is None:
            self._publishing = MessageBusFactory(
                self._config, [self._transports], self._handlers, named=self._named
            ).bus()
        return self._publishing

    def _select(self, names: Sequence[str]) -> dict[str, TransportConfig]:
        """Return the configured transports among ``names``.

        Raises:
            UnknownTransportError: If a name is neither configured nor registered.
        """
        transports = self._config.transports
        missing = tuple(n for n in names if n not in transports and n not in self._receivers)
        if missing:
            raise UnknownTransportError(missing, (*transports, *self._receivers))
        return {name: transports[name] for name in names if name in transports}


def _receivers_of(built: Mapping[str, SenderInterface]) -> dict[str, ReceiverInterface]:
    """Return the receive half of what a factory built, by transport name.

    Raises:
        NotConsumableError: If a transport sends but cannot be consumed,
            which means its adapter should have provided a worker instead.
    """
    receivers: dict[str, ReceiverInterface] = {
        name: made for name, made in built.items() if isinstance(made, ReceiverInterface)
    }
    if len(receivers) != len(built):
        raise NotConsumableError(tuple(built), "transport")
    return receivers


def _one_receiver(
    receivers: Sequence[ReceiverInterface], names: Sequence[str]
) -> ReceiverInterface:
    """Return the single receiver, or one draining them all, each message named by origin."""
    return receivers[0] if len(receivers) == 1 else ChainedReceiver(receivers, names)
