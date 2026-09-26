"""Handles a :class:`~xtr_messenger.message.RedispatchMessage` by dispatching what it carries."""

from __future__ import annotations

from typing import final

from xtr_messenger.envelope import Envelope
from xtr_messenger.message import RedispatchMessage
from xtr_messenger.message_bus_interface import MessageBusInterface
from xtr_messenger.stamp import HandledStamp, NonSendableStampInterface, TransportNamesStamp

__all__ = ["RedispatchMessageHandler"]


@final
class RedispatchMessageHandler:
    """Dispatches the envelope a :class:`RedispatchMessage` carries, through ``bus``.

    ``bus`` must be one that routes — the bus a publishing process uses —
    or the envelope would only be handled here again. Stamps that describe
    this process — a :class:`~xtr_messenger.stamp.ReceivedStamp` above all,
    which would stop routing — are stripped first: the envelope is going out
    as new.
    """

    __slots__ = ("_bus",)

    def __init__(self, bus: MessageBusInterface) -> None:
        """Dispatch through ``bus``."""
        self._bus = bus

    async def __call__(self, message: RedispatchMessage) -> object:
        """Dispatch the carried envelope, to the named transports if any.

        Returns:
            What the handler of the carried message returned, when it was
            handled rather than sent.
        """
        envelope = Envelope.wrap(message.envelope).without_stamps(NonSendableStampInterface)
        names = message.transport_names
        stamps = (TransportNamesStamp(names),) if names else ()
        dispatched = await self._bus.dispatch(envelope, *stamps)
        handled = dispatched.last(HandledStamp)
        return handled.result if handled is not None else None
