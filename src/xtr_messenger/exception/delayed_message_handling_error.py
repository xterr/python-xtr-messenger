"""Messages held back until the current one was handled failed once dispatched."""

from __future__ import annotations

from typing import TYPE_CHECKING

from xtr_messenger._text import safe_str

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
        failures: Each held-back message that failed, with what its dispatch
            raised, in the order dispatched.
        errors: What each failed dispatch raised, in the same order.
    """

    envelope: Envelope
    failures: tuple[tuple[Envelope, Exception], ...]
    errors: tuple[Exception, ...]

    def __init__(self, envelope: Envelope, failures: Sequence[tuple[Envelope, Exception]]) -> None:
        """Record the message that succeeded and each held-back message that then failed."""
        self.envelope = envelope
        self.failures = tuple(failures)
        self.errors = tuple(error for _, error in self.failures)
        reasons = "; ".join(safe_str(error) for error in self.errors)
        kind = type(envelope.message).__qualname__
        super().__init__(
            f"{len(self.errors)} message(s) dispatched after {kind} was handled failed: {reasons}"
        )
