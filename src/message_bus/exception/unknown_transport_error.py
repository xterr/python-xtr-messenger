"""A route names a transport that was never registered."""

from __future__ import annotations

from .message_bus_error import MessageBusError

__all__ = ["UnknownTransportError"]


class UnknownTransportError(MessageBusError):
    """A routing entry names a transport that was never registered."""

    transport_name: str
    known_transports: tuple[str, ...]

    def __init__(self, transport_name: str, known_transports: tuple[str, ...]) -> None:
        """Record the missing transport alongside the registered ones."""
        self.transport_name = transport_name
        self.known_transports = known_transports
        known = ", ".join(known_transports) or "<none>"
        super().__init__(f"unknown transport {transport_name!r}; registered: {known}")
