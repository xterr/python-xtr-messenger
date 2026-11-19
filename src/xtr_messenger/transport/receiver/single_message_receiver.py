"""A receiver handing out one message another receiver holds."""

from __future__ import annotations

from typing import TYPE_CHECKING, final, override

from .receiver_interface import ReceiverInterface
from .retrying_receiver_interface import RetryingReceiverInterface

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from xtr_messenger.envelope import Envelope

__all__ = ["SingleMessageReceiver"]


@final
class SingleMessageReceiver(ReceiverInterface, RetryingReceiverInterface):
    """Yields ``envelope`` once, then is exhausted; settles it through ``receiver``.

    How one message found in a receiver — by
    :meth:`~xtr_messenger.transport.receiver.ListableReceiverInterface.find`,
    say — is handed to a worker on its own, and settled where it is kept.
    """

    __slots__ = ("_envelope", "_received", "_receiver")

    def __init__(self, receiver: ReceiverInterface, envelope: Envelope) -> None:
        """Hand out ``envelope``, which ``receiver`` holds."""
        self._receiver = receiver
        self._envelope = envelope
        self._received = False

    @override
    async def get(self) -> AsyncIterator[Envelope]:
        """Yield the envelope the first time; nothing after."""
        if self._received:
            return
        self._received = True
        yield self._envelope

    @override
    async def ack(self, envelope: Envelope) -> None:
        """Acknowledge ``envelope`` through the receiver that holds it."""
        await self._receiver.ack(envelope)

    @override
    async def reject(self, envelope: Envelope) -> None:
        """Reject ``envelope`` through the receiver that holds it."""
        await self._receiver.reject(envelope)

    @override
    def will_retry(self, envelope: Envelope) -> bool:
        """Tell what the receiver that holds ``envelope`` does with it once rejected."""
        receiver = self._receiver
        return isinstance(receiver, RetryingReceiverInterface) and receiver.will_retry(envelope)
