"""One receive loop over several receivers."""

from __future__ import annotations

from itertools import count
from typing import TYPE_CHECKING, final

from typing_extensions import override

from xtr_messenger.stamp import AckReceiptStamp, ReceivedStamp

from .receiver_interface import ReceiverInterface

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Sequence

    from xtr_messenger.envelope import Envelope

__all__ = ["ChainedReceiver"]


@final
class ChainedReceiver(ReceiverInterface):
    """Drains several receivers in turn, settling each message on its origin.

    A worker runs one loop, but a connection often carries several named
    queues. This presents them as one receiver, then routes every
    acknowledgement back to the receiver the message actually came from.

    Correlation does not rely on the order things are settled in. Each
    message gets a ticket of this receiver's own, and the origin is recorded
    against it, so a caller may hold a message and settle it whenever —
    including after later messages have already been settled.
    """

    __slots__ = ("_names", "_outstanding", "_receivers", "_tickets")

    def __init__(
        self,
        receivers: Sequence[ReceiverInterface],
        names: Sequence[str] | None = None,
    ) -> None:
        """Drain ``receivers`` in the order given.

        ``names``, one per receiver, are the transport names they serve: each
        message is stamped with a :class:`~xtr_messenger.stamp.ReceivedStamp`
        naming the transport it came from, so a worker draining several can
        tell which one a message belongs to.

        Raises:
            ValueError: If ``names`` does not name every receiver exactly once.
        """
        self._receivers = tuple(receivers)
        self._names = tuple(names) if names is not None else None
        if self._names is not None and len(self._names) != len(self._receivers):
            raise ValueError("ChainedReceiver needs one name per receiver")
        self._outstanding: dict[int, tuple[ReceiverInterface, Envelope]] = {}
        self._tickets = count(1)

    @override
    async def get(self) -> AsyncIterator[Envelope]:
        """Yield from each receiver in turn, tagging every message with a ticket."""
        for position, receiver in enumerate(self._receivers):
            named = () if self._names is None else (ReceivedStamp(self._names[position]),)
            async for envelope in receiver.get():
                ticket = next(self._tickets)
                self._outstanding[ticket] = (receiver, envelope)
                yield envelope.with_stamps(*named, AckReceiptStamp(ticket))

    @override
    async def ack(self, envelope: Envelope) -> None:
        """Acknowledge ``envelope`` on the receiver it came from."""
        origin = self._take(envelope)
        if origin is not None:
            receiver, collected = origin
            await receiver.ack(collected)

    @override
    async def reject(self, envelope: Envelope) -> None:
        """Reject ``envelope`` on the receiver it came from.

        Forwards the envelope as handed over rather than as collected, so
        anything the consumer added on the way survives, with the origin's
        own receipt restored as the one that counts.
        """
        origin = self._take(envelope)
        if origin is None:
            return
        receiver, collected = origin
        receipt = collected.last(AckReceiptStamp)
        await receiver.reject(envelope if receipt is None else envelope.with_stamps(receipt))

    def _take(self, envelope: Envelope) -> tuple[ReceiverInterface, Envelope] | None:
        ticket = envelope.last(AckReceiptStamp)
        if ticket is None:
            return None
        return self._outstanding.pop(ticket.receipt, None)
