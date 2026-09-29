"""The library's own receive loop."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Protocol, final, runtime_checkable

from typing_extensions import override

from ._handling import Failed, Handled, Skipped, announce_failure, handle, receiver_name_of
from .event import WorkerRunningEvent, WorkerStartedEvent, WorkerStoppedEvent
from .stamp import ReceivedStamp
from .transport.receiver._close_stream import close_stream
from .worker_interface import WorkerInterface

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from xtr_event_dispatcher_contracts import EventDispatcherInterface

    from .envelope import Envelope
    from .message_bus_interface import MessageBusInterface
    from .transport.receiver.receiver_interface import ReceiverInterface

__all__ = ["AsyncResetter", "Worker"]


@runtime_checkable
class AsyncResetter(Protocol):
    """Something with ``async reset()`` — the shape ``ServicesResetter`` fits.

    A structural protocol lets the worker call the kernel's resetter without
    importing the container package, so a messenger process without a kernel
    keeps its dependency count.
    """

    async def reset(self) -> None:
        """Reset every service tracked since the last reset."""
        ...


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

    __slots__ = (
        "_bus",
        "_dispatcher",
        "_receiver",
        "_receiver_name",
        "_resetter",
        "_stop_requested",
        "_stopped",
    )

    def __init__(
        self,
        bus: MessageBusInterface,
        receiver: ReceiverInterface,
        resetter: AsyncResetter | None = None,
        *,
        event_dispatcher: EventDispatcherInterface | None = None,
        receiver_name: str | None = None,
    ) -> None:
        """Dispatch messages collected from ``receiver`` through ``bus``.

        ``resetter``, when given, has ``reset()`` awaited after each handled
        message — settled either way — so long-lived services (a logger's
        fingers-crossed buffer, a request-scoped registry) are cleared between
        units of work.

        ``event_dispatcher``, when given, hears about the worker and about
        every message — see :mod:`xtr_messenger.event`. ``receiver_name`` is
        the transport name those events report, and the one each message's
        :class:`~xtr_messenger.stamp.ReceivedStamp` carries to its handlers;
        left out, each envelope's own stamp names it, which is how a worker
        draining several transports reports each one.
        """
        self._bus = bus
        self._receiver = receiver
        self._resetter = resetter
        self._dispatcher = event_dispatcher
        self._receiver_name = receiver_name
        self._stopped: asyncio.Event | None = None
        self._stop_requested = False

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

        With an event dispatcher, a message a listener decides not to handle
        is acknowledged without being dispatched — skipping is not failing.
        A listener raising while a message is received or handled fails that
        message like a handler would; one raising while a failure is
        announced is a bug in the listener, so the message is rejected and the
        exception stops the worker. Exceptions from the started, running and
        stopped listeners propagate too.
        """
        stopped = asyncio.Event()
        if self._stop_requested:
            self._stop_requested = False
            stopped.set()
        self._stopped = stopped
        await self._announce(WorkerStartedEvent(self))
        collected = self._receiver.get()
        try:
            while (envelope := await _next_unless_stopped(collected, stopped)) is not None:
                await self._settle(envelope)
                await self._announce(WorkerRunningEvent(self))
        finally:
            self._stopped = None
            await close_stream(collected)
            await self._announce(WorkerStoppedEvent(self))

    @override
    def stop(self) -> None:
        """Ask the worker to finish the message in hand and return.

        Asked while not running — before :meth:`run` has started, say — the
        next run returns without collecting anything.
        """
        if self._stopped is not None:
            self._stopped.set()
        else:
            self._stop_requested = True

    async def _settle(self, envelope: Envelope) -> None:
        name = receiver_name_of(envelope, self._receiver_name)
        received = envelope.last(ReceivedStamp)
        if self._receiver_name is not None and (
            received is None or received.transport_name != self._receiver_name
        ):
            # A receiver names what it is, not the transport it was configured as: the
            # handlers see the name the worker was built for, as they do draining several.
            envelope = envelope.with_stamps(ReceivedStamp(self._receiver_name))
        try:
            match await handle(self._bus, envelope, name, self._dispatcher):
                case Handled(settled) | Skipped(settled):
                    await self._receiver.ack(settled)
                case Failed() as failed:
                    await announce_failure(failed, name, self._dispatcher, self._receiver.reject)
        finally:
            if self._resetter is not None:
                await self._resetter.reset()

    async def _announce(self, event: object) -> None:
        if self._dispatcher is not None:
            _ = await self._dispatcher.dispatch(event)


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
            # Waited for, so the receiver is at rest before it is closed, and a
            # message it delivered just as it was cancelled is still settled.
            _ = await asyncio.gather(fetch, return_exceptions=True)
    return None if fetch.cancelled() else fetch.result()
