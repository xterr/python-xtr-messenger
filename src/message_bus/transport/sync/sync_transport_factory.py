"""Building ``sync://`` transports from a DSN."""

from __future__ import annotations

from typing import TYPE_CHECKING, final

from typing_extensions import override

from message_bus.transport.transport_factory_interface import TransportFactoryInterface
from message_bus.transport.transport_options import reject_unknown_options

from .sync_transport import SyncTransport

if TYPE_CHECKING:
    from collections.abc import Mapping

    from message_bus.dsn import Dsn
    from message_bus.transport.sender import SenderInterface
    from message_bus.transport.transport_config import TransportConfig

__all__ = ["SYNC_OPTIONS", "SYNC_SCHEME", "SyncTransportFactory"]

SYNC_SCHEME = "sync"

#: A sync transport has nothing to configure: it handles where it dispatches.
SYNC_OPTIONS: tuple[str, ...] = ()


@final
class SyncTransportFactory(TransportFactoryInterface):
    """Builds ``sync://`` transports, which handle in the calling process."""

    __slots__ = ()

    @override
    def supports(self, dsn: Dsn) -> bool:
        """Recognise the ``sync`` scheme."""
        return dsn.scheme == SYNC_SCHEME

    @override
    def create(self, group: Mapping[str, TransportConfig]) -> Mapping[str, SenderInterface]:
        """Build a sync transport per name.

        Raises:
            UnknownTransportOptionError: If a DSN carries any setting, since
                a sync transport has nothing to configure.
        """
        for spec in group.values():
            reject_unknown_options(SYNC_SCHEME, spec.settings, SYNC_OPTIONS)
        return {name: SyncTransport() for name in group}
