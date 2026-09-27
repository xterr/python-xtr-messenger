"""One message's way through a worker, announced to listeners.

Shared by the library's own :class:`~xtr_messenger.worker.Worker` and by the
taskiq binding, which settle a message differently — one tells a receiver, the
other returns or raises to taskiq — but must announce it the same way.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, final

from .envelope import Envelope
from .event import (
    WorkerMessageFailedEvent,
    WorkerMessageHandledEvent,
    WorkerMessageReceivedEvent,
)
from .exception import HandlersFailedError
from .stamp import ErrorDetailsStamp, ReceivedStamp

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from xtr_event_dispatcher_contracts import EventDispatcherInterface

    from .message_bus_interface import MessageBusInterface

__all__ = ["Failed", "Handled", "Skipped", "announce_failure", "handle", "receiver_name_of"]


@final
@dataclass(frozen=True, slots=True)
class Handled:
    """The message was handled; ``envelope`` is what to acknowledge."""

    envelope: Envelope


@final
@dataclass(frozen=True, slots=True)
class Skipped:
    """A listener decided the message is not to be handled; acknowledge ``envelope``."""

    envelope: Envelope


@final
@dataclass(frozen=True, slots=True)
class Failed:
    """Handling raised ``error``; ``envelope`` is the last one known before it did."""

    envelope: Envelope
    error: Exception


def receiver_name_of(envelope: Envelope, default: str | None = None) -> str:
    """Return ``default``, or else the transport name the envelope was received from."""
    if default is not None:
        return default
    received = envelope.last(ReceivedStamp)
    return received.transport_name if received is not None else ""


async def handle(
    bus: MessageBusInterface,
    envelope: Envelope,
    receiver_name: str,
    dispatcher: EventDispatcherInterface | None,
) -> Handled | Skipped | Failed:
    """Announce ``envelope`` as received, dispatch it unless skipped, announce it handled.

    An exception from the bus, or from a listener of the received or handled
    event, is a failure of this message and comes back as :class:`Failed`
    rather than raised: one bad message must not stop a worker, and a listener
    that raises on it is part of how that message was dealt with.
    """
    current = envelope
    try:
        if dispatcher is not None:
            received = await dispatcher.dispatch(WorkerMessageReceivedEvent(current, receiver_name))
            current = received.envelope
            if not received.should_handle():
                return Skipped(current)
        current = await bus.dispatch(current)
        if dispatcher is not None:
            handled = await dispatcher.dispatch(WorkerMessageHandledEvent(current, receiver_name))
            current = handled.envelope
    except Exception as error:  # noqa: BLE001 — reported to the caller, which settles the message
        return Failed(current, error)
    return Handled(current)


async def announce_failure(
    failed: Failed,
    receiver_name: str,
    dispatcher: EventDispatcherInterface | None,
    settle: Callable[[Envelope], Awaitable[None]],
    *,
    will_retry: bool = False,
) -> None:
    """Stamp why ``failed`` failed, announce it, then ``settle`` the envelope listeners left.

    A listener raising here is a bug in the listener rather than a problem
    with the message, so it is not caught — but the message is settled
    first, since a collected message must be settled whatever happens.
    """
    error = failed.error
    # What went wrong is what a handler raised, not that handlers failed.
    cause = next(iter(error.errors.values())) if isinstance(error, HandlersFailedError) else error
    envelope = failed.envelope.with_stamps(ErrorDetailsStamp(type(cause).__name__, str(cause)))
    if dispatcher is None:
        await settle(envelope)
        return
    event = WorkerMessageFailedEvent(envelope, receiver_name, error, will_retry=will_retry)
    try:
        _ = await dispatcher.dispatch(event)
    finally:
        await settle(event.envelope)
