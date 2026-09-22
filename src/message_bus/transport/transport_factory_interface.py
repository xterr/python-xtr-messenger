"""The contract for building transports from a DSN.

A transport is named and described by a DSN in configuration, so swapping
``sync://`` for ``amqp://…`` in an environment file needs no code change.

Each factory receives every transport sharing a DSN at once, so an adapter
that holds a connection can build one and hand it to all of them.

Implementations live beside the transport they build — ``transport/sync/``,
``transport/in_memory/`` — or, when they need a driver, under ``bridge/``.
That split is deliberate: everything under ``transport/`` imports with no
optional dependency installed, so resolving a ``sync://`` transport never
drags a broker library in behind it.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, runtime_checkable

from message_bus.exception import UnsupportedDsnError

if TYPE_CHECKING:
    from collections.abc import Mapping

    from message_bus.dsn import Dsn
    from message_bus.transport.sender import SenderInterface

    from .transport_config import TransportConfig

__all__ = ["TransportFactoryInterface", "UnsupportedDsnError"]


@runtime_checkable
class TransportFactoryInterface(Protocol):
    """Builds both halves of the transports described by one connection.

    One question, asked of every adapter: given these transports, build
    what sends to them. What *consumes* them is a separate concern, because
    most transports are consumed by the library's own loop while a few bring
    a worker of their own — see
    :class:`~message_bus.worker_providing_interface.WorkerProvidingInterface`.
    """

    def supports(self, dsn: Dsn) -> bool:
        """Report whether this factory recognises the scheme of ``dsn``."""
        ...

    def create(self, group: Mapping[str, TransportConfig]) -> Mapping[str, SenderInterface]:
        """Build a sender for every named transport in ``group``.

        Every spec in ``group`` addresses the same server, so a factory that
        owns a connection can create one and share it across them.
        """
        ...
