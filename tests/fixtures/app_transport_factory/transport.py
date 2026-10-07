"""An app-provided transport factory for a private ``counting://`` scheme.

Defined in a scanned module and implementing
:class:`~xtr_messenger.TransportFactoryInterface`, so the bundle's
autoconfiguration registers it and puts it ahead of the entry-point factories
discovery finds. It also closes publishers like a broker-backed factory does,
so a test can see *which* kernel's factory a shutdown reached.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, final

from typing_extensions import override

from xtr_messenger import (
    Dsn,
    PublisherClosingInterface,
    TransportConfig,
    TransportFactoryInterface,
)
from xtr_messenger.transport.in_memory import InMemoryTransport

if TYPE_CHECKING:
    from collections.abc import Mapping

    from xtr_messenger.transport.sender import SenderInterface


@final
class CountingTransportFactory(TransportFactoryInterface, PublisherClosingInterface):
    """Serves ``counting://``, recording each build and each publisher close.

    One singleton serves both the bus and the worker: it counts each build and
    caches one recorder per name, so the worker drains exactly what the bus
    sent through it. Mutable because that recording is its purpose.
    """

    def __init__(self) -> None:
        self.created = 0
        self.closed = 0
        self._made: dict[str, InMemoryTransport] = {}

    @override
    def supports(self, dsn: Dsn) -> bool:
        return dsn.scheme == "counting"

    @override
    def create(self, group: Mapping[str, TransportConfig]) -> Mapping[str, SenderInterface]:
        self.created += 1
        return {name: self._transport(name) for name in group}

    @override
    async def close_publishers(self) -> None:
        self.closed += 1

    def _transport(self, name: str) -> InMemoryTransport:
        made = self._made.get(name)
        if made is None:
            made = InMemoryTransport()
            self._made[name] = made
        return made
