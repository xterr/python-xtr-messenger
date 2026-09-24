"""A transport was asked for by a name the configuration does not define."""

from __future__ import annotations

from .message_bus_error import MessageBusError

__all__ = ["UnknownTransportError"]


class UnknownTransportError(MessageBusError):
    """A route or a worker names a transport the configuration does not define."""

    names: tuple[str, ...]
    known: tuple[str, ...]

    def __init__(self, names: tuple[str, ...], known: tuple[str, ...]) -> None:
        """Record what was asked for, and what is actually configured."""
        self.names = names
        self.known = known
        defined = ", ".join(sorted(known)) or "<none>"
        super().__init__(f"unknown transport(s): {', '.join(names)}; defined: {defined}")
