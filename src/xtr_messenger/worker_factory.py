"""Building what a worker process runs."""

from __future__ import annotations

from typing import TYPE_CHECKING, final

from .exception import NotConsumableError, UnknownTransportError
from .message_bus import MessageBus
from .middleware import HandleMessageMiddleware
from .transport.receiver.chained_receiver import ChainedReceiver
from .transport.receiver.receiver_interface import ReceiverInterface
from .transport.transport_factory import TransportFactory
from .worker import Worker
from .worker_providing_interface import WorkerProvidingInterface

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from .handler import HandlersLocatorInterface
    from .message_bus_config import MessageBusConfig
    from .message_bus_interface import MessageBusInterface
    from .middleware.middleware_interface import MiddlewareInterface
    from .transport.sender import SenderInterface
    from .transport.transport_config import TransportConfig
    from .transport.transport_factory_interface import TransportFactoryInterface
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

    __slots__ = ("_bus", "_config", "_handlers", "_middleware", "_transports")

    def __init__(
        self,
        config: MessageBusConfig,
        factories: Sequence[TransportFactoryInterface] | None = None,
        handlers: HandlersLocatorInterface | None = None,
        bus: MessageBusInterface | None = None,
        middleware: Sequence[MiddlewareInterface] = (),
    ) -> None:
        """Build from ``config``; ``bus`` overrides the one built for handling.

        ``handlers`` replaces the process-wide registry in the bus this
        worker dispatches through, which is the only place a handler is
        called — an adapter bringing its own worker, as the AMQP one does,
        dispatches into that bus too.

        ``middleware`` runs on every message collected, ahead of handling —
        where a :class:`~xtr_messenger.middleware.LoggingMiddleware` goes, so a
        consuming process reports what it handled. It is ignored when ``bus``
        is given, that bus being composed already.
        """
        self._config = config
        self._transports = TransportFactory(factories)
        self._handlers = handlers
        self._bus = bus
        self._middleware = tuple(middleware)

    def worker(self, names: Sequence[str]) -> WorkerInterface:
        """Build the worker for exactly the named transports.

        Import the modules that declare your handlers first — a handler that
        has not been declared cannot be found.

        Raises:
            UnknownTransportError: If a name is not configured.
            UnsupportedDsnError: If no factory recognises their DSN.
        """
        group = self._select(names)
        factory = self._transports.serving(group)
        bus = self._dispatcher()
        if isinstance(factory, WorkerProvidingInterface):
            return factory.worker(group, bus)
        return Worker(bus, _receiver_of(factory.create(group)))

    def _dispatcher(self) -> MessageBusInterface:
        """Return the bus a collected message is dispatched through.

        Handling only. An envelope that arrived from a transport carries a
        :class:`~xtr_messenger.stamp.ReceivedStamp` and is deliberately never
        routed again, so a worker's bus would build every sender in the
        configuration and then never use one — on AMQP that is a second
        connection per worker, opened and idle.

        A handler that publishes does so through the bus it was given, which
        is the producing one built by
        :class:`~xtr_messenger.message_bus_factory.MessageBusFactory`. Pass
        ``bus`` to supply that here instead.
        """
        if self._bus is not None:
            return self._bus
        return MessageBus([*self._middleware, HandleMessageMiddleware(self._handlers)])

    def _select(self, names: Sequence[str]) -> dict[str, TransportConfig]:
        transports = self._config.transports
        missing = tuple(name for name in names if name not in transports)
        if missing:
            raise UnknownTransportError(missing, tuple(transports))
        return {name: transports[name] for name in names}


def _receiver_of(built: Mapping[str, SenderInterface]) -> ReceiverInterface:
    """Return the receive half of what a factory built.

    Raises:
        NotConsumableError: If a transport sends but cannot be consumed,
            which means its adapter should have provided a worker instead.
    """
    receivers = [made for made in built.values() if isinstance(made, ReceiverInterface)]
    if len(receivers) != len(built):
        raise NotConsumableError(tuple(built), "transport")
    return receivers[0] if len(receivers) == 1 else ChainedReceiver(receivers)
