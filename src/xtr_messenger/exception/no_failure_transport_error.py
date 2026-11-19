"""A failed message was looked for with no failure transport to look in."""

from __future__ import annotations

from .message_bus_error import MessageBusError

__all__ = ["NoFailureTransportError"]


class NoFailureTransportError(MessageBusError):
    """No failure transport was named, and the configuration has no default one."""

    known: tuple[str, ...]

    def __init__(self, known: tuple[str, ...]) -> None:
        """Record the failure transports there are to choose from."""
        self.known = known
        available = ", ".join(known) or "<none>"
        reason = "no failure transport was named and none is the default"
        super().__init__(f"{reason}; failure transports: {available}")
