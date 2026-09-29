"""Holds a message back until the one being handled is done with."""

from __future__ import annotations

from dataclasses import dataclass

from .non_sendable_stamp_interface import NonSendableStampInterface

__all__ = ["DispatchAfterCurrentBusStamp"]


@dataclass(frozen=True, slots=True)
class DispatchAfterCurrentBusStamp(NonSendableStampInterface):
    """Dispatch the message only once the message being handled was handled successfully.

    A handler that asks for a follow-up — a mail, an event another process
    consumes — usually means "once what I did is final". Stamped with this,
    the follow-up waits until every handler of the current message, and every
    middleware around them — a database transaction's commit included — has
    finished; if that fails, it is never dispatched. Dispatched with nothing
    being handled, it goes out at once.
    """
