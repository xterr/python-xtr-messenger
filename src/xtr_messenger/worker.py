"""The library's own receive loop."""

from __future__ import annotations

import asyncio
import logging
from contextvars import copy_context
from typing import TYPE_CHECKING, Final, Protocol, final, override, runtime_checkable

from ._handling import Failed, Handled, Skipped, announce_failure, handle, receiver_name_of
from ._own_context import in_own_context
from .event import WorkerRunningEvent, WorkerStartedEvent, WorkerStoppedEvent
from .stamp import ReceivedStamp
from .transport.receiver._close_stream import close_stream
from .transport.receiver.retrying_receiver_interface import RetryingReceiverInterface
from .worker_interface import WorkerInterface

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from contextvars import Context

    from xtr_event_dispatcher_contracts import EventDispatcherInterface

    from .envelope import Envelope
    from .failure.failure_transport_sender import FailureTransportSender
    from .message_bus_interface import MessageBusInterface
    from .transport.receiver.receiver_interface import ReceiverInterface

__all__ = ["AsyncResetter", "Worker"]

#: Log lines go through the messenger channel, so an application capturing
#: the standard library's logging sees them beside the rest of the bus's
#: output.
_LOGGER: Final = logging.getLogger("messenger")


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
    backoff state — so a message that fails here and will be delivered again
    is rejected, and the transport retries it. Adding a retry here would not
    replace that; it would run *underneath* it, and every failure would be
    attempted the product of both policies.

    A message that failed for good — no attempt left, by the transport's
    :class:`~xtr_messenger.transport.receiver.RetryingReceiverInterface`, or
    any failure on a transport that does not retry — goes to the failure
    transport configured for its transport: sent there, then acknowledged.
    With none, it is rejected, and the transport keeps it its own way.
    """

    __slots__ = (
        "_bus",
        "_dispatcher",
        "_failure_transport_sender",
        "_receiver",
        "_receiver_name",
        "_resetter",
        "_stop_requested",
        "_stopped",
    )

    def __init__(  # noqa: PLR0913 — everything past `resetter` is keyword-only
        self,
        bus: MessageBusInterface,
        receiver: ReceiverInterface,
        resetter: AsyncResetter | None = None,
        *,
        event_dispatcher: EventDispatcherInterface | None = None,
        receiver_name: str | None = None,
        failure_transport_sender: FailureTransportSender | None = None,
    ) -> None:
        """Dispatch messages collected from ``receiver`` through ``bus``.

        ``resetter``, when given, has ``reset()`` awaited after each handled
        message — settled either way — so long-lived services (a logger's
        fingers-crossed buffer) are cleared between units of work. A reset
        that fails is logged: it changes nothing about the message, already
        settled, and does not stop the worker.

        ``event_dispatcher``, when given, hears about the worker and about
        every message — see :mod:`xtr_messenger.event`. ``receiver_name`` is
        the transport name those events report, and the one each message's
        :class:`~xtr_messenger.stamp.ReceivedStamp` carries to its handlers;
        left out, each envelope's own stamp names it, which is how a worker
        draining several transports reports each one.

        ``failure_transport_sender``, when given, sends a message that failed
        for good to the failure transport configured for the transport it
        came from; it is then acknowledged. Without one — or for a transport
        with none configured, a message that failed in a failure transport
        already, or a send that fails — it is rejected.
        """
        self._bus = bus
        self._receiver = receiver
        self._resetter = resetter
        self._dispatcher = event_dispatcher
        self._receiver_name = receiver_name
        self._failure_transport_sender = failure_transport_sender
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

        Each message is handled — announced, dispatched, settled — in a
        context of its own, apart from the one this runs in. Whatever runs a
        worker is not its messages': a unit of work open there, a command's,
        would be joined by what a message does once its own unit is over —
        the messages it held back, a listener of its events — and shared by
        every message after it. Nor does a context variable one message left
        behind reach the next. Collecting stays where this runs: only a
        message is new work. It runs in one context for the whole run,
        copied from this one, so a receiver may set a context variable before
        it yields and reset it once the worker comes back for the next.

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
        collecting = copy_context()
        try:
            while (
                envelope := await _next_unless_stopped(collected, stopped, collecting)
            ) is not None:
                await in_own_context(self._settle(envelope))
                await self._announce(WorkerRunningEvent(self))
        finally:
            self._stopped = None
            await asyncio.get_running_loop().create_task(
                close_stream(collected), context=collecting
            )
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
                    receiver = self._receiver
                    will_retry = isinstance(
                        receiver, RetryingReceiverInterface
                    ) and receiver.will_retry(failed.envelope)
                    await announce_failure(
                        failed,
                        name,
                        self._dispatcher,
                        lambda settled, retried: self._settle_failure(settled, name, retried),
                        will_retry=will_retry,
                    )
        finally:
            if self._resetter is not None:
                try:
                    await self._resetter.reset()
                except Exception:
                    _LOGGER.exception("services failed to reset after a message")

    async def _settle_failure(self, envelope: Envelope, name: str, will_retry: bool) -> None:
        """Settle ``envelope``, which failed on ``name``, once listeners have had their say.

        One that comes back — the transport retries it, or a listener said a
        retry is arranged — is rejected. One that failed for good is sent to
        its failure transport and acknowledged, and rejected when it has none
        or the send did not happen: acknowledged only once it is kept there.
        """
        sender = self._failure_transport_sender
        if not will_retry and sender is not None and await sender.send(envelope, name):
            await self._receiver.ack(envelope)
            return
        await self._receiver.reject(envelope)

    async def _announce(self, event: object) -> None:
        if self._dispatcher is not None:
            _ = await self._dispatcher.dispatch(event)


async def _next_unless_stopped(
    collected: AsyncIterator[Envelope],
    stopped: asyncio.Event,
    collecting: Context,
) -> Envelope | None:
    """Return the next envelope, or ``None`` once exhausted or stopped.

    Waits on both at once, so a stop is honoured while the receiver is still
    waiting for a message. A message that arrives together with the stop is
    returned rather than dropped: it was collected, so it has to be settled.
    Every step of ``collected`` runs in ``collecting``, the one context the
    receiver's generator sees from its first step to its last.

    Raises:
        Exception: Whatever the receiver raised while collecting.
    """
    if stopped.is_set():
        return None
    fetch = asyncio.get_running_loop().create_task(_next(collected), context=collecting)
    # Waiting on an event reads no context; it is cancelled before this returns.
    # ast-grep-ignore: task-without-context
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


async def _next(collected: AsyncIterator[Envelope]) -> Envelope | None:
    """Return the next envelope ``collected`` yields, or ``None`` once it is exhausted."""
    return await anext(collected, None)
