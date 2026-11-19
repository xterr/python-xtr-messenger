"""What a receiver that can list its messages, and find one, offers."""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, runtime_checkable

from .receiver_interface import ReceiverInterface

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from xtr_messenger.envelope import Envelope

__all__ = ["ListableReceiverInterface"]


@runtime_checkable
class ListableReceiverInterface(ReceiverInterface, Protocol):
    """A receiver whose messages can be looked into without being handed out.

    Every envelope it hands back carries a
    :class:`~xtr_messenger.stamp.TransportMessageIdStamp` with the id
    :meth:`find` takes, and whatever else :meth:`get` would stamp it with to
    settle it — so a listed envelope can be acknowledged or rejected like a
    received one.
    """

    def all(self, limit: int | None = None) -> AsyncIterator[Envelope]:
        """Yield the messages waiting in this receiver, ``limit`` at most."""
        ...

    async def find(self, message_id: str) -> Envelope | None:
        """Return the message kept as ``message_id``, or ``None`` when there is none."""
        ...
