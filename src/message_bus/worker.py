"""The library's own receive loop."""

from __future__ import annotations

from typing import TYPE_CHECKING, final

from typing_extensions import override

from .stamp import ErrorDetailsStamp
from .worker_interface import WorkerInterface

if TYPE_CHECKING:
    from .message_bus_interface import MessageBusInterface
    from .transport.receiver.receiver_interface import ReceiverInterface

__all__ = ["Worker"]


@final
class Worker(WorkerInterface):
    """Pulls from a receiver and dispatches each message through a bus.

    The loop is the whole implementation: collect an envelope, dispatch it,
    acknowledge it if that returned, reject it if it raised.

    **This makes exactly one attempt per message and never retries.** That is
    a deliberate division of labour, not an omission. Redelivery is something
    only the transport can do correctly — it owns the delivery count, the
    backoff state, and the dead-letter destination — so a message that fails
    here is rejected and the transport decides what happens next. Adding a
    retry here would not replace that; it would run *underneath* it, and
    every failure would be attempted the product of both policies.
    """

    __slots__ = ("_bus", "_receiver")

    def __init__(self, bus: MessageBusInterface, receiver: ReceiverInterface) -> None:
        """Dispatch messages collected from ``receiver`` through ``bus``."""
        self._bus = bus
        self._receiver = receiver

    @override
    async def run(self) -> None:
        """Handle each collected message, settling it either way.

        Returns when the receiver stops yielding. Cancelling the task running
        this closes the receiver's iterator, which is how a worker shuts down
        without abandoning a message mid-flight — cancellation is not an
        ``Exception``, so it propagates rather than being mistaken for a
        message that failed.

        A message that raises is rejected and the loop moves on: one
        undeliverable message must not take the worker down with it. The
        failure is not lost in the process — it is attached to the rejected
        envelope as an
        :class:`~message_bus.stamp.ErrorDetailsStamp`, so whatever the
        transport does with a rejection carries the reason with it.
        """
        async for envelope in self._receiver.get():
            try:
                _ = await self._bus.dispatch(envelope.message, *envelope.stamps)
            except Exception as error:  # noqa: BLE001 — one bad message must not stop the worker
                details = ErrorDetailsStamp(type(error).__name__, str(error))
                await self._receiver.reject(envelope.with_stamps(details))
                continue
            await self._receiver.ack(envelope)
