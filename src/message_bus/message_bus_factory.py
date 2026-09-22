"""Building the bus a publishing process dispatches through."""

from __future__ import annotations

from typing import TYPE_CHECKING, final

from .message_bus import MessageBus
from .middleware.handle_message_middleware import HandleMessageMiddleware
from .middleware.send_message_middleware import SendMessageMiddleware
from .transport.sender import SendersLocator
from .transport.transport_factory import TransportFactory

if TYPE_CHECKING:
    from collections.abc import Sequence

    from .handler import HandlersLocatorInterface
    from .message_bus_config import MessageBusConfig
    from .middleware.middleware_interface import MiddlewareInterface
    from .transport.sender import SenderInterface, SendersLocatorInterface
    from .transport.transport_factory_interface import TransportFactoryInterface

__all__ = ["MessageBusFactory"]


@final
class MessageBusFactory:
    """Builds the bus a publishing process dispatches through.

    Pass ``factories`` to control how transports are constructed — a
    serializer of your own, a private handlers locator, or a scheme the
    library does not ship.
    """

    __slots__ = ("_config", "_handlers", "_transports")

    def __init__(
        self,
        config: MessageBusConfig,
        factories: Sequence[TransportFactoryInterface] | None = None,
        handlers: HandlersLocatorInterface | None = None,
    ) -> None:
        """Build from ``config``; ``factories`` overrides lazy discovery.

        ``handlers`` configures handling on this bus. It does **not** reach
        transport factories, which discovery builds with no arguments — a
        transport that resolves handlers itself, as ``sync://`` does, will
        use the process-wide registry regardless. Give a private registry to
        the factory instead::

            MessageBusFactory(config, [SyncTransportFactory(handlers=private)])
        """
        self._config = config
        self._transports = TransportFactory(factories)
        self._handlers = handlers

    def bus(
        self,
        middleware: Sequence[MiddlewareInterface] = (),
        require_sender: bool = False,
        handles: bool = False,
    ) -> MessageBus:
        """Build a bus that routes according to the configuration.

        ``middleware`` runs first; the send middleware goes last, because
        nothing should come between routing and the transport. Set
        ``require_sender`` to reject a message no transport is routed for
        rather than letting the dispatch pass quietly.

        Set ``handles`` for the bus a worker dispatches through. It appends
        handling *after* routing, which is the order that lets one
        composition serve both sides: a message being published is routed and
        stops there, while one that arrived from a transport carries a
        :class:`~message_bus.stamp.ReceivedStamp`, is deliberately not routed
        again, and falls through to be handled here.

        Raises:
            UnsupportedDsnError: If no factory recognises a transport's DSN.
        """
        send = SendMessageMiddleware(self._locator(), require_sender=require_sender)
        chain: list[MiddlewareInterface] = [*middleware, send]
        if handles:
            chain.append(HandleMessageMiddleware(self._handlers))
        return MessageBus(chain)

    def _locator(self) -> SendersLocatorInterface:
        return SendersLocator(self._config.routing, self._senders())

    def _senders(self) -> dict[str, SenderInterface]:
        built: dict[str, SenderInterface] = {}
        for group in self._config.by_connection().values():
            built.update(self._transports.create(group))
        return built
