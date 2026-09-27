"""One receive loop over several receivers."""

from __future__ import annotations

import asyncio
from collections import deque
from itertools import count
from typing import TYPE_CHECKING, final

from typing_extensions import override

from xtr_messenger.stamp import AckReceiptStamp, ReceivedStamp

from ._close_stream import close_stream
from .receiver_interface import ReceiverInterface

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Sequence
    from typing import TypeAlias

    from xtr_messenger.envelope import Envelope

__all__ = ["ChainedReceiver"]

if TYPE_CHECKING:
    _Fetch: TypeAlias = "asyncio.Task[Envelope]"


@final
class ChainedReceiver(ReceiverInterface):
    """Collects from several receivers at once, settling each message on its origin.

    A worker runs one loop, but a connection often carries several named
    queues. This presents them as one receiver, then routes every
    acknowledgement back to the receiver the message actually came from.

    Every receiver is waited on at the same time, so one that never runs dry
    — a broker subscription — does not keep the others from being served.
    Messages come in the order they arrive; each receiver's own order is
    kept.

    Correlation does not rely on the order things are settled in. Each
    message gets a ticket of this receiver's own, and the origin is recorded
    against it, so a caller may hold a message and settle it whenever —
    including after later messages have already been settled.
    """

    __slots__ = ("_leftovers", "_names", "_outstanding", "_receivers", "_tickets")

    def __init__(
        self,
        receivers: Sequence[ReceiverInterface],
        names: Sequence[str] | None = None,
    ) -> None:
        """Collect from ``receivers``, those given first served first when several are ready.

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
        self._leftovers: deque[tuple[int, Envelope]] = deque()

    @override
    async def get(self) -> AsyncIterator[Envelope]:
        """Yield what every receiver delivers as it arrives, tagging each with a ticket.

        Stopped while waiting, every receiver's pending wait is cancelled. A
        message one of them delivered in that same moment is kept, and handed
        out first by the next :meth:`get`, rather than lost.
        """
        while self._leftovers:
            position, envelope = self._leftovers.popleft()
            yield self._ticketed(position, envelope)

        streams = [receiver.get() for receiver in self._receivers]
        fetching: dict[_Fetch, int] = {
            _fetch(stream): position for position, stream in enumerate(streams)
        }
        try:
            while fetching:
                done, _ = await asyncio.wait(fetching, return_when=asyncio.FIRST_COMPLETED)
                for task in sorted(done, key=fetching.__getitem__):
                    position = fetching.pop(task)
                    try:
                        envelope = task.result()
                    except StopAsyncIteration:
                        continue
                    fetching[_fetch(streams[position])] = position
                    yield self._ticketed(position, envelope)
        finally:
            await self._abandon(fetching)
            for stream in streams:
                await close_stream(stream)

    def _ticketed(self, position: int, envelope: Envelope) -> Envelope:
        """Record where ``envelope`` came from, and return it stamped with its ticket."""
        ticket = next(self._tickets)
        self._outstanding[ticket] = (self._receivers[position], envelope)
        named = () if self._names is None else (ReceivedStamp(self._names[position]),)
        return envelope.with_stamps(*named, AckReceiptStamp(ticket))

    async def _abandon(self, fetching: dict[_Fetch, int]) -> None:
        """Cancel the waits still pending; keep what one delivered as it was stopped."""
        for task in fetching:
            _ = task.cancel()
        # Gathered rather than awaited one by one: a receiver's own failure,
        # met while stopping, must not take the place of the stop.
        outcomes = await asyncio.gather(*fetching, return_exceptions=True)
        for position, outcome in zip(fetching.values(), outcomes, strict=True):
            if not isinstance(outcome, BaseException):
                self._leftovers.append((position, outcome))

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


def _fetch(stream: AsyncIterator[Envelope]) -> _Fetch:
    """Start waiting for ``stream``'s next message."""
    return asyncio.ensure_future(anext(stream))
