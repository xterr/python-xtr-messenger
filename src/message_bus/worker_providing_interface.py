"""The contract for a transport that brings its own worker."""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from collections.abc import Mapping

    from .message_bus_interface import MessageBusInterface
    from .transport.transport_config import TransportConfig
    from .worker_interface import WorkerInterface

__all__ = ["WorkerProvidingInterface"]


@runtime_checkable
class WorkerProvidingInterface(Protocol):
    """A transport factory whose broker consumes on its own terms.

    Most transports are whole — they send and they receive — so a worker is
    simply :class:`~message_bus.worker.Worker` driving the receive half, and
    the factory need not be involved. A few brokers invert that: they own the
    consume loop and call handlers themselves, so nothing can drive them from
    outside and only the adapter can build what a worker runs.

    Implement this only in that second case.
    :class:`~message_bus.worker_factory.WorkerFactory` prefers it when
    present and falls back to driving the transport itself, which is what
    keeps the common case free of this.
    """

    def worker(
        self,
        group: Mapping[str, TransportConfig],
        bus: MessageBusInterface,
    ) -> WorkerInterface:
        """Build what a worker process runs to consume exactly ``group``."""
        ...
