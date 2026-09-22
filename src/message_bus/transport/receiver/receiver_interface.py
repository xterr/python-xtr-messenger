"""The contract for the consuming half of a transport."""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from message_bus.envelope import Envelope

__all__ = ["ReceiverInterface"]


@runtime_checkable
class ReceiverInterface(Protocol):
    """A transport's receive half: pull messages, then settle them.

    Every envelope yielded by :meth:`get` must eventually be settled exactly
    once, by :meth:`ack` when it was handled and :meth:`reject` when it was
    not. Until then the broker is entitled to consider it outstanding.

    Envelopes arriving here carry a
    :class:`~message_bus.stamp.ReceivedStamp`, which stops them being routed
    back out — a consumer cannot re-publish what it consumes.

    **Divergence from Symfony.** Symfony's ``get()`` returns an iterable that
    a worker polls in a loop, because PHP has no persistent async runtime to
    hold a subscription open. Python does, so :meth:`get` is an async
    iterator instead: it maps directly onto how brokers actually deliver,
    cancellation gives graceful shutdown for free, and prefetch stays the
    broker's business rather than something a poll interval has to
    approximate.
    """

    def get(self) -> AsyncIterator[Envelope]:
        """Yield envelopes as the transport delivers them.

        Yields until cancelled or until the transport is exhausted. Each
        envelope carries whatever handle the transport needs to settle it
        later, so it must be passed back to :meth:`ack` or :meth:`reject`
        rather than reconstructed.
        """
        ...

    async def ack(self, envelope: Envelope) -> None:
        """Tell the transport ``envelope`` was handled and may be dropped."""
        ...

    async def reject(self, envelope: Envelope) -> None:
        """Tell the transport ``envelope`` failed terminally.

        Terminally is the operative word: whatever retry policy applies has
        already run by the time this is called, so implementations should
        move the message somewhere it can be inspected rather than putting it
        back in line to fail again.
        """
        ...
