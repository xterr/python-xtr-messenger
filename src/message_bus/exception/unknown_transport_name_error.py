"""A name was asked for that the configuration does not define."""

from __future__ import annotations

from .message_bus_error import MessageBusError

__all__ = ["UnknownTransportNameError"]


class UnknownTransportNameError(MessageBusError):
    """A worker or route named a transport the configuration does not define."""

    requested: tuple[str, ...]
    defined: tuple[str, ...]

    def __init__(self, requested: tuple[str, ...], defined: tuple[str, ...]) -> None:
        """Record what was asked for, and what is actually configured."""
        self.requested = requested
        self.defined = defined
        known = ", ".join(sorted(defined)) or "<none>"
        super().__init__(f"undefined transport(s): {', '.join(requested)}. Defined: {known}")
