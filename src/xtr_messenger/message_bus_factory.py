"""Building the bus a publishing process dispatches through."""

from __future__ import annotations

from typing import TYPE_CHECKING, final

from .message_bus import MessageBus
from .middleware.handle_message_middleware import HandleMessageMiddleware
from .middleware.named import chain, named_middleware
from .middleware.send_message_middleware import SendMessageMiddleware
from .transport.sender import SendersLocator
from .transport.transport_factory import TransportFactory

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from xtr_logging_contracts import LoggerInterface

    from .handler import HandlersLocatorInterface
    from .message_bus_config import MessageBusConfig
    from .middleware.middleware_interface import MiddlewareInterface
    from .middleware.named import MiddlewareBuilder
    from .transport.sender import SenderInterface, SendersLocatorInterface
    from .transport.transport_factory_interface import TransportFactoryInterface

__all__ = ["MessageBusFactory"]


@final
class MessageBusFactory:
    """Builds the bus a publishing process dispatches through.

    Pass ``factories`` to control how transports are constructed — a
    serializer of your own, or a scheme the library does not ship.
    """

    __slots__ = ("_config", "_handlers", "_named", "_transports")

    def __init__(
        self,
        config: MessageBusConfig,
        factories: Sequence[TransportFactoryInterface] | None = None,
        handlers: HandlersLocatorInterface | None = None,
        *,
        logger: LoggerInterface | None = None,
        named: Mapping[str, MiddlewareBuilder] | None = None,
    ) -> None:
        """Build from ``config``; ``factories`` overrides lazy discovery.

        ``handlers`` replaces the process-wide registry for everything this
        bus handles, ``sync://`` included: no transport calls a handler
        itself, they all hand the message back to the bus.

        ``logger`` is what the ``"logging"`` middleware writes through when
        the configuration names it. ``named`` adds names of your own for the
        configuration to use, each mapped to what builds it.
        """
        self._config = config
        self._transports = TransportFactory(factories)
        self._handlers = handlers
        self._named = named_middleware(logger, named)

    def bus(self) -> MessageBus:
        """Build a bus that routes according to the configuration.

        What the configuration's ``middleware`` names runs first; routing goes
        after it, because nothing should come between routing and the
        transport.

        Handling always comes *after* routing, which is the order that lets
        one composition serve both sides: a message being published is routed
        and stops there, while one that arrived from a transport — or was
        handed straight back by ``sync://`` — carries a
        :class:`~xtr_messenger.stamp.ReceivedStamp`, is deliberately not routed
        again, and falls through to be handled here.

        ``default_middleware=False`` in the configuration leaves routing and
        handling off altogether, for a bus composed entirely by hand.

        Raises:
            UnknownMiddlewareError: If the configuration names middleware
                ``named`` has nothing registered for.
            UnsupportedDsnError: If no factory recognises a transport's DSN.
        """
        return MessageBus(chain(self._config, self._named, self._routing_and_handling))

    def _routing_and_handling(self) -> list[MiddlewareInterface]:
        send = SendMessageMiddleware(
            self._locator(),
            require_sender=self._config.require_sender,
            handle_unrouted=self._config.handle_unrouted,
        )
        return [send, HandleMessageMiddleware(self._handlers)]

    def _locator(self) -> SendersLocatorInterface:
        return SendersLocator(self._config.routing, self._senders())

    def _senders(self) -> dict[str, SenderInterface]:
        built: dict[str, SenderInterface] = {}
        for group in self._config.by_connection().values():
            built.update(self._transports.create(group))
        return built
