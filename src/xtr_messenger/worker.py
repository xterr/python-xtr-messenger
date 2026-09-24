"""The library's own receive loop."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, final

from typing_extensions import override

from .stamp import ErrorDetailsStamp
from .worker_interface import WorkerInterface

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from .envelope import Envelope
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

    __slots__ = ("_bus", "_receiver", "_stopped")

    def __init__(self, bus: MessageBusInterface, receiver: ReceiverInterface) -> None:
        """Dispatch messages collected from ``receiver`` through ``bus``."""
        self._bus = bus
        self._receiver = receiver
        self._stopped: asyncio.Event | None = None

    @override
    async def run(self) -> None:
        """Handle each collected message, settling it either way.

        Returns when the receiver stops yielding, or once :meth:`stop` is
        called — at once if the worker is waiting for a message, after
        settling it if one is in hand. Cancelling the task running this stops
        it too, and cancellation is not an ``Exception``, so it propagates
        rather than being mistaken for a message that failed.

        A message that raises is rejected and the loop moves on: one
        undeliverable message must not take the worker down with it. The
        failure is not lost in the process — it is attached to the rejected
        envelope as an
        :class:`~xtr_messenger.stamp.ErrorDetailsStamp`, so whatever the
        transport does with a rejection carries the reason with it.
        """
        stopped = asyncio.Event()
        self._stopped = stopped
        collected = self._receiver.get()
        try:
            while (envelope := await _next_unless_stopped(collected, stopped)) is not None:
                await self._settle(envelope)
        finally:
            self._stopped = None

    @override
    def stop(self) -> None:
        """Ask a running worker to finish the message in hand and return."""
        if self._stopped is not None:
            self._stopped.set()

    async def _settle(self, envelope: Envelope) -> None:
        try:
            _ = await self._bus.dispatch(envelope)
        except Exception as error:  # noqa: BLE001 — one bad message must not stop the worker
            details = ErrorDetailsStamp(type(error).__name__, str(error))
            await self._receiver.reject(envelope.with_stamps(details))
            return
        await self._receiver.ack(envelope)


async def _next_unless_stopped(
    collected: AsyncIterator[Envelope],
    stopped: asyncio.Event,
) -> Envelope | None:
    """Return the next envelope, or ``None`` once exhausted or stopped.

    Waits on both at once, so a stop is honoured while the receiver is still
    waiting for a message. A message that arrives together with the stop is
    returned rather than dropped: it was collected, so it has to be settled.

    Raises:
        Exception: Whatever the receiver raised while collecting.
    """
    if stopped.is_set():
        return None
    fetch = asyncio.ensure_future(anext(collected, None))
    halt = asyncio.ensure_future(stopped.wait())
    try:
        _ = await asyncio.wait((fetch, halt), return_when=asyncio.FIRST_COMPLETED)
    finally:
        _ = halt.cancel()
        if not fetch.done():
            _ = fetch.cancel()
    return fetch.result() if fetch.done() else None
