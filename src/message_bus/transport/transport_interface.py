"""The contract for a transport that both sends and receives."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from message_bus.transport.sender import SenderInterface

from .receiver.receiver_interface import ReceiverInterface

__all__ = ["TransportInterface"]


@runtime_checkable
class TransportInterface(SenderInterface, ReceiverInterface, Protocol):
    """Both halves of a transport: what sends, and what receives.

    Deliberately empty. The two halves are useful apart — a publishing
    process needs only :class:`~message_bus.transport.sender.SenderInterface`,
    and a worker only
    :class:`~message_bus.transport.receiver.receiver_interface.ReceiverInterface` — so
    they stay separately implementable and separately mockable. This name
    exists for the common case where one object is both, and for code that
    wants to say so in a signature.

    Stating it as a base class rather than relying on structural matching
    means conformance is checked where the transport is defined, instead of
    at whatever distant call site first passes the wrong shape.
    """
