"""What a factory bringing its own worker offers to send final failures to a failure transport."""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from collections.abc import Mapping

    from xtr_event_dispatcher_contracts import EventDispatcherInterface

    from .failure.failure_transport_sender import FailureTransportSender
    from .message_bus_interface import MessageBusInterface
    from .transport.transport_config import TransportConfig
    from .worker import AsyncResetter
    from .worker_interface import WorkerInterface

__all__ = ["FailureTransportWorkerProvidingInterface"]


@runtime_checkable
class FailureTransportWorkerProvidingInterface(Protocol):
    """A worker-providing factory whose worker sends final failures to a failure transport.

    Implemented beside
    :class:`~xtr_messenger.worker_providing_interface.WorkerProvidingInterface`:
    :class:`~xtr_messenger.worker_factory.WorkerFactory` calls
    :meth:`worker_with_failure_transport` instead of ``worker()`` when a
    transport of the group has a failure transport configured. A factory
    implementing ``worker()`` alone keeps working; its final failures stay
    with the broker, and the worker factory says so in a warning.
    """

    def worker_with_failure_transport(
        self,
        group: Mapping[str, TransportConfig],
        bus: MessageBusInterface,
        failure_transport_sender: FailureTransportSender,
        *,
        event_dispatcher: EventDispatcherInterface | None = None,
        resetter: AsyncResetter | None = None,
    ) -> WorkerInterface:
        """Build what ``worker()`` builds, final failures sent through ``failure_transport_sender``.

        On a message's last attempt — known to be the last, never guessed —
        the worker calls ``await failure_transport_sender.send(envelope,
        transport_name)``; when that returns ``True`` it acknowledges the
        delivery instead of dead-lettering it, and when it returns ``False``
        it settles the message as ``worker()`` does. A message a listener set
        for retry is not failed for good.
        """
        ...
