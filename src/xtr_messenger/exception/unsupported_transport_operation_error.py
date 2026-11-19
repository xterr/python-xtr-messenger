"""A transport was asked for something it cannot do."""

from __future__ import annotations

from .message_bus_error import MessageBusError

__all__ = ["UnsupportedTransportOperationError"]


class UnsupportedTransportOperationError(MessageBusError):
    """A transport cannot list, count, find or receive, and was asked to.

    What a transport can do beyond sending is what it implements: listing and
    finding messages is
    :class:`~xtr_messenger.transport.receiver.ListableReceiverInterface`,
    receiving is
    :class:`~xtr_messenger.transport.receiver.ReceiverInterface`.
    """

    transport: str
    operation: str

    def __init__(self, transport: str, operation: str) -> None:
        """Record the transport and what it was asked to do — ``"list messages"``, say."""
        self.transport = transport
        self.operation = operation
        super().__init__(f"the {transport!r} transport cannot {operation}")
