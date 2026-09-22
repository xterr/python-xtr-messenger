"""Delegating to whichever factory recognises a DSN."""

from __future__ import annotations

from typing import TYPE_CHECKING, final

from typing_extensions import override

from message_bus.exception import UnsupportedDsnError

from .transport_factory_discovery import factory_for
from .transport_factory_interface import TransportFactoryInterface

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from message_bus.dsn import Dsn
    from message_bus.transport.sender import SenderInterface

    from .transport_config import TransportConfig

__all__ = ["TransportFactory"]


@final
class TransportFactory(TransportFactoryInterface):
    """Builds transports by asking the factory that recognises their DSN.

    A factory itself, so everything that builds a transport depends on one
    type whether there is a single adapter behind it or a dozen. That is what
    lets a bus be handed one collaborator rather than a list plus the rules
    for choosing from it.

    Built with no arguments it **discovers** adapters by DSN scheme, loading
    only the one a scheme needs — an application speaking ``sync://`` never
    imports a broker library. Given a list it uses exactly those, in order,
    which is how you supply an adapter that needs a collaborator discovery
    cannot provide::

        TransportFactory([AmqpTransportFactory(serializer=mine)])
    """

    __slots__ = ("_discovered", "_factories")

    def __init__(self, factories: Sequence[TransportFactoryInterface] | None = None) -> None:
        """Delegate to ``factories`` in order, or to discovery when omitted."""
        self._factories = None if factories is None else tuple(factories)
        self._discovered: dict[str, TransportFactoryInterface] = {}

    @override
    def supports(self, dsn: Dsn) -> bool:
        """Report whether any factory behind this one recognises ``dsn``."""
        if self._factories is None:
            return self._discover(dsn) is not None
        return any(factory.supports(dsn) for factory in self._factories)

    @override
    def create(self, group: Mapping[str, TransportConfig]) -> Mapping[str, SenderInterface]:
        """Build the senders for ``group`` with the factory that serves it.

        Raises:
            UnsupportedDsnError: If nothing recognises the scheme.
        """
        return self.serving(group).create(group)

    def _discover(self, dsn: Dsn) -> TransportFactoryInterface | None:
        """Return the factory serving ``dsn``, discovering it at most once.

        Kept because a factory holds what it builds — the broker for a
        connection, the recorder behind a name. Discovering a fresh one per
        call would hand a worker and the bus it dispatches through two
        objects for the same server, which is the opposite of the sharing a
        connection is for.
        """
        known = self._discovered.get(dsn.scheme)
        if known is None:
            known = factory_for(dsn)
            if known is not None:
                self._discovered[dsn.scheme] = known
        return known

    def serving(self, group: Mapping[str, TransportConfig]) -> TransportFactoryInterface:
        """Return the factory that recognises ``group``'s DSN.

        Public because building a worker needs to know *which* adapter is in
        play — some bring their own, most do not.

        Raises:
            UnsupportedDsnError: If nothing recognises the scheme.
        """
        name = next(iter(group))
        dsn = group[name].parsed
        if self._factories is None:
            discovered = self._discover(dsn)
            if discovered is not None:
                return discovered
        else:
            for factory in self._factories:
                if factory.supports(dsn):
                    return factory
        raise UnsupportedDsnError(name, dsn.raw)
