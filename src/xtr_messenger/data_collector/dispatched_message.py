"""One message a profiled request dispatched, as the panel shows it."""

from __future__ import annotations

from dataclasses import dataclass
from typing import final

__all__ = ["DispatchedMessage"]


@final
@dataclass(slots=True)
class DispatchedMessage:
    """What one dispatch carried, what came of it, and how long it took.

    Recorded as the dispatch starts, so messages keep the order they were
    dispatched in — a message a handler dispatches comes after the one it
    is handling — and completed once the bus answers. Every value is plain
    data already, cloned and masked when recorded: the message and the
    handlers' results may change, or be gone, by the time the profile is
    collected.

    Attributes:
        message_class: The message's qualified class name.
        message: The message, as a :func:`~xtr_http_kernel.data_collector.clone_var`
            tree with credential-named values masked.
        stamps_in: The class names of the stamps the dispatch was given.
        stamps_out: The class names of the stamps on the envelope the bus
            handed back — on a failure, the one the failure carried.
        handled: One ``{"handler", "result"}`` per handler that ran to the end.
        sent: One ``{"sender", "alias"}`` per sender that accepted the message.
        duration_ms: How long the dispatch took; ``None`` while it runs.
        error: What the dispatch raised, ``None`` when it did not.
    """

    message_class: str
    message: dict[str, object]
    stamps_in: tuple[str, ...]
    stamps_out: tuple[str, ...] = ()
    handled: tuple[dict[str, object], ...] = ()
    sent: tuple[dict[str, str], ...] = ()
    duration_ms: float | None = None
    error: dict[str, object] | None = None

    def to_data(self) -> dict[str, object]:
        """Return the dispatch as the JSON-safe mapping a profile keeps."""
        return {
            "message_class": self.message_class,
            "message": self.message,
            "stamps_in": list(self.stamps_in),
            "stamps_out": list(self.stamps_out),
            "handled": [dict(entry) for entry in self.handled],
            "sent": [dict(entry) for entry in self.sent],
            "duration_ms": self.duration_ms,
            "error": self.error,
        }
