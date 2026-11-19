"""Every transport, by the name it is consumed and configured under."""

from __future__ import annotations

from typing import TYPE_CHECKING, final

from xtr_messenger.exception import UnknownTransportError

from .transport_factory import TransportFactory

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from xtr_messenger.message_bus_config import MessageBusConfig

    from .receiver.receiver_interface import ReceiverInterface
    from .sender.sender_interface import SenderInterface
    from .transport_factory_interface import TransportFactoryInterface

__all__ = ["TransportLocator"]


@final
class TransportLocator:
    """Hands over each transport by name: the configured ones, then the registered receivers.

    What the console commands reach a transport through — to set it up, count
    or list its messages, or read its failed ones. A configured transport is
    built the first time it is asked for, by the factory that serves its DSN,
    so building this does no I/O; a factory that keeps what it builds hands
    back the very transport the bus sends to. A registered receiver — one that
    only receives, and so has no DSN — is handed over as it was registered; a
    configured transport of the same name wins, as it does for a worker.
    """

    __slots__ = ("_built", "_config", "_receivers", "_transports")

    def __init__(
        self,
        config: MessageBusConfig,
        factories: Sequence[TransportFactoryInterface] | None = None,
        *,
        receivers: Mapping[str, ReceiverInterface] | None = None,
    ) -> None:
        """Build ``config``'s transports with ``factories``, beside ``receivers``.

        ``factories`` means what it does for
        :class:`~xtr_messenger.message_bus_factory.MessageBusFactory`: left
        out, the factory serving each DSN is discovered by its scheme.
        """
        self._config = config
        self._transports = TransportFactory(factories)
        self._receivers: dict[str, ReceiverInterface] = dict(receivers or {})
        self._built: dict[str, SenderInterface | ReceiverInterface] = {}

    def names(self) -> tuple[str, ...]:
        """Return every name, the configured transports first, in the order they were given."""
        configured = tuple(self._config.transports)
        return (*configured, *(name for name in self._receivers if name not in configured))

    def has(self, name: str) -> bool:
        """Tell whether a transport or a receiver goes by ``name``."""
        return name in self._config.transports or name in self._receivers

    def get(self, name: str) -> SenderInterface | ReceiverInterface:
        """Return the transport ``name``, building it the first time.

        Raises:
            UnknownTransportError: If nothing goes by ``name``.
            UnsupportedDsnError: If no factory recognises its DSN.
        """
        built = self._built.get(name)
        if built is not None:
            return built
        configured = self._config.transports.get(name)
        if configured is not None:
            built = self._transports.create({name: configured})[name]
        elif name in self._receivers:
            built = self._receivers[name]
        else:
            raise UnknownTransportError((name,), self.names())
        self._built[name] = built
        return built
