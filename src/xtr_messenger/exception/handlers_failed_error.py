"""One or more handlers of a message raised."""

from __future__ import annotations

from typing import TYPE_CHECKING

from .message_bus_error import MessageBusError

if TYPE_CHECKING:
    from collections.abc import Mapping

    from xtr_messenger.envelope import Envelope

__all__ = ["HandlersFailedError"]


class HandlersFailedError(MessageBusError):
    """One or more handlers of a message raised, after every handler had its turn.

    One failing handler does not keep the others from running: each is
    independent, and an unrelated one — an audit trail, a metric — must not go
    quiet because another broke. What every handler that failed raised is
    kept, by handler name, and the first of them is the cause.

    Attributes:
        envelope: The message as handling left it, with a
            :class:`~xtr_messenger.stamp.HandledStamp` for every handler that
            succeeded.
        errors: What each failed handler raised, by handler name, in the
            order the handlers ran; a name another failed handler already
            took is told apart as ``name#2``, ``name#3``….
    """

    envelope: Envelope
    errors: Mapping[str, Exception]

    def __init__(self, envelope: Envelope, errors: Mapping[str, Exception]) -> None:
        """Record the envelope handling left and what each failed handler raised."""
        self.envelope = envelope
        self.errors = dict(errors)
        failures = "; ".join(f"{name}: {error}" for name, error in self.errors.items())
        kind = type(envelope.message).__qualname__
        super().__init__(f"{len(self.errors)} handler(s) of {kind} failed: {failures}")
