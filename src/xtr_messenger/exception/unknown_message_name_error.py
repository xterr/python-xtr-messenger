"""A wire name could not be resolved back to a message type."""

from __future__ import annotations

from .message_bus_error import MessageBusError

__all__ = ["UnknownMessageNameError"]


class UnknownMessageNameError(MessageBusError):
    """A wire message name could not be resolved back to a Python type."""

    name: str

    def __init__(self, name: str) -> None:
        """Record the unresolvable name."""
        self.name = name
        remedy = "import the module that defines it, or declare it with @as_message"
        super().__init__(f"no message type registered for name {name!r}; {remedy}")
