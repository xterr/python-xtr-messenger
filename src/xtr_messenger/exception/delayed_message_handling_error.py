"""Messages held back until the current one was handled failed once dispatched."""

from __future__ import annotations

from typing import TYPE_CHECKING

from .message_bus_error import MessageBusError

if TYPE_CHECKING:
    from collections.abc import Sequence

    from xtr_messenger.envelope import Envelope

__all__ = ["DelayedMessageHandlingError"]


class DelayedMessageHandlingError(MessageBusError):
    """One or more messages held back with a ``DispatchAfterCurrentBusStamp`` failed.

    The message being handled succeeded — what it did stands — and then the
    messages its handling held back were dispatched, each in turn, every one
    tried even when an earlier one failed. The first failure is the cause.

    Attributes:
        envelope: The message whose handling held them back, as its handling
            left it.
        errors: What each failed dispatch raised, in the order dispatched.
    """

    envelope: Envelope
    errors: tuple[Exception, ...]

    def __init__(self, envelope: Envelope, errors: Sequence[Exception]) -> None:
        """Record the message that succeeded and what its held-back messages raised."""
        self.envelope = envelope
        self.errors = tuple(errors)
        failures = "; ".join(str(error) for error in self.errors)
        kind = type(envelope.message).__qualname__
        super().__init__(
            f"{len(self.errors)} message(s) dispatched after {kind} was handled failed: {failures}"
        )
