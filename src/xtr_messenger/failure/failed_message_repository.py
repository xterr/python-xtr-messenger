"""The messages kept in the failure transports: listed, found, removed, dispatched again."""

from __future__ import annotations

from typing import TYPE_CHECKING, final

from xtr_messenger.exception import (
    NoFailureTransportError,
    UnknownTransportError,
    UnsupportedTransportOperationError,
)
from xtr_messenger.stamp import (
    NonSendableStampInterface,
    SentToFailureTransportStamp,
    TransportMessageIdStamp,
)
from xtr_messenger.transport.receiver.listable_receiver_interface import (
    ListableReceiverInterface,
)
from xtr_messenger.transport.receiver.message_count_aware_interface import (
    MessageCountAwareInterface,
)
from xtr_messenger.transport.receiver.receiver_interface import ReceiverInterface

from .failed_message_filter import FailedMessageFilter

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Sequence

    from xtr_messenger.envelope import Envelope
    from xtr_messenger.message_bus_interface import MessageBusInterface
    from xtr_messenger.transport.transport_locator import TransportLocator

__all__ = ["FailedMessageRepository"]


@final
class FailedMessageRepository:
    """Reads and removes the messages kept in the failure transports.

    What the ``messenger:failed:*`` commands are built on, so listing,
    inspecting, removing and dispatching again a failed message can be done
    from outside the console too.

    Every method takes the failure transport to read by name; left out, it is
    the configuration's own ``failure_transport``.
    """

    __slots__ = ("_bus", "_global_transport_name", "_transport_names", "_transports")

    def __init__(
        self,
        transports: TransportLocator,
        bus: MessageBusInterface,
        transport_names: Sequence[str],
        global_transport_name: str | None = None,
    ) -> None:
        """Read the failure transports ``transport_names`` out of ``transports``.

        Args:
            transports: Where each transport is reached by name.
            bus: What :meth:`redispatch` dispatches a message again through.
            transport_names: The transports that are failure transports.
            global_transport_name: The one read when none is named.
        """
        self._transports = transports
        self._transport_names = tuple(transport_names)
        self._global_transport_name = global_transport_name
        self._bus = bus

    @staticmethod
    def message_id(envelope: Envelope) -> str | None:
        """Return the id ``envelope`` is kept under in its transport, if it carries one."""
        stamp = envelope.last(TransportMessageIdStamp)
        return stamp.message_id if stamp is not None else None

    @staticmethod
    def prepare_for_redispatch(envelope: Envelope) -> Envelope:
        """Return ``envelope`` without what must not travel with it when dispatched again.

        What only meant something where it was kept goes — what this process
        stamped on it, the id it was kept under — and so does the mark of its
        failure: dispatched again, it is a message like any other.
        """
        return (
            envelope.without_stamps(NonSendableStampInterface)
            .without_stamps(TransportMessageIdStamp)
            .without_stamps(SentToFailureTransportStamp)
        )

    @property
    def transport_names(self) -> tuple[str, ...]:
        """Return every failure transport, the configuration's own first."""
        return self._transport_names

    @property
    def global_transport_name(self) -> str | None:
        """Return the failure transport read when none is named, if there is one."""
        return self._global_transport_name

    def supports_listing(self, transport: str | None = None) -> bool:
        """Tell whether the failure transport can list and find its messages.

        Raises:
            NoFailureTransportError: If ``transport`` is left out and there is
                no default failure transport.
            UnknownTransportError: If ``transport`` is not a failure transport.
        """
        return isinstance(self._transport(transport), ListableReceiverInterface)

    async def count(self, transport: str | None = None) -> int | None:
        """Return how many messages the failure transport keeps, or ``None`` if it cannot count.

        Raises:
            NoFailureTransportError: If ``transport`` is left out and there is
                no default failure transport.
            UnknownTransportError: If ``transport`` is not a failure transport.
        """
        found = self._transport(transport)
        return await found.count() if isinstance(found, MessageCountAwareInterface) else None

    async def find(self, message_id: str, transport: str | None = None) -> Envelope | None:
        """Return the failed message kept as ``message_id``, or ``None`` when there is none.

        Raises:
            NoFailureTransportError: If ``transport`` is left out and there is
                no default failure transport.
            UnknownTransportError: If ``transport`` is not a failure transport.
            UnsupportedTransportOperationError: If it cannot find a message.
        """
        return await self._listable(transport).find(message_id)

    def all(
        self,
        transport: str | None = None,
        selected: FailedMessageFilter | None = None,
        limit: int | None = None,
    ) -> AsyncIterator[Envelope]:
        """Yield the failed messages ``selected`` selects, ``limit`` of them at most.

        The failure transport is resolved now, so one that cannot be read is
        reported by this call rather than by the first step of what it returns.

        Raises:
            NoFailureTransportError: If ``transport`` is left out and there is
                no default failure transport.
            UnknownTransportError: If ``transport`` is not a failure transport.
            UnsupportedTransportOperationError: If it cannot list messages.
        """
        chosen = selected if selected is not None else FailedMessageFilter()
        return chosen.select(self._listable(transport), limit)

    async def remove(self, envelope: Envelope, transport: str | None = None) -> None:
        """Drop ``envelope`` from the failure transport for good.

        By acknowledging it: rejecting it would hand it back to the
        transport's own retries, which is what retrying it does.

        Raises:
            NoFailureTransportError: If ``transport`` is left out and there is
                no default failure transport.
            UnknownTransportError: If ``transport`` is not a failure transport.
            UnsupportedTransportOperationError: If it does not receive.
        """
        await self.receiver(transport).ack(envelope)

    async def redispatch(self, envelope: Envelope, transport: str | None = None) -> None:
        """Dispatch ``envelope`` again, then drop it from the failure transport.

        Dropped only once dispatched: a dispatch that raises leaves it where
        it is, to be dispatched once what failed is fixed.

        Raises:
            MessageBusError: Whatever dispatching raises, or what :meth:`remove`
                does.
        """
        receiver = self.receiver(transport)
        _ = await self._bus.dispatch(self.prepare_for_redispatch(envelope))
        await receiver.ack(envelope)

    def receiver(self, transport: str | None = None) -> ReceiverInterface:
        """Return the failure transport as the receiver it must be to be read.

        Raises:
            NoFailureTransportError: If ``transport`` is left out and there is
                no default failure transport.
            UnknownTransportError: If ``transport`` is not a failure transport.
            UnsupportedTransportOperationError: If it does not receive.
        """
        name = self._name(transport)
        found = self._transports.get(name)
        if not isinstance(found, ReceiverInterface):
            raise UnsupportedTransportOperationError(name, "receive messages")
        return found

    def _listable(self, transport: str | None) -> ListableReceiverInterface:
        name = self._name(transport)
        found = self._transports.get(name)
        if not isinstance(found, ListableReceiverInterface):
            raise UnsupportedTransportOperationError(name, "list messages")
        return found

    def _transport(self, transport: str | None) -> object:
        return self._transports.get(self._name(transport))

    def _name(self, transport: str | None) -> str:
        name = transport if transport is not None else self._global_transport_name
        if name is None:
            raise NoFailureTransportError(self._transport_names)
        if name not in self._transport_names:
            raise UnknownTransportError((name,), self._transport_names)
        return name
