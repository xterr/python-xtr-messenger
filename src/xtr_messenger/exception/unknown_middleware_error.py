"""Middleware was named by a name nothing is registered under."""

from __future__ import annotations

from .message_bus_error import MessageBusError

__all__ = ["UnknownMiddlewareError"]


class UnknownMiddlewareError(MessageBusError):
    """The configuration names middleware this process has nothing registered for."""

    name: str
    known: tuple[str, ...]

    def __init__(self, name: str, known: tuple[str, ...]) -> None:
        """Record what was named, and what a name may actually be."""
        self.name = name
        self.known = known
        registered = ", ".join(sorted(known)) or "<none>"
        super().__init__(f"unknown middleware: {name}; registered: {registered}")
