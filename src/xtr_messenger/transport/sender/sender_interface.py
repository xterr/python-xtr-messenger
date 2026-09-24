"""The contract for the sending half of a transport."""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from xtr_messenger.envelope import Envelope

__all__ = ["SenderInterface"]


@runtime_checkable
class SenderInterface(Protocol):
    """A transport's send half."""

    async def send(self, envelope: Envelope) -> Envelope:
        """Hand ``envelope`` to the transport and return it, possibly re-stamped.

        Implementations typically append a
        :class:`~xtr_messenger.stamp.TransportMessageIdStamp`.
        """
        ...
