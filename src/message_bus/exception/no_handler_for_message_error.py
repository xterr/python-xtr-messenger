"""No handler is registered for a message that reached handling."""

from __future__ import annotations

from .message_bus_error import MessageBusError

__all__ = ["NoHandlerForMessageError"]


class NoHandlerForMessageError(MessageBusError):
    """No handler is registered for a message that reached handling.

    In a worker this almost always means the module declaring the handler was
    never imported, so the registration decorator never ran. Failing is the
    safe reading: the alternative is acknowledging the message and losing it
    silently.
    """

    message_type: type
    handled_types: tuple[str, ...]

    def __init__(self, message_type: type, handled_types: tuple[str, ...]) -> None:
        """Record the unhandled type alongside the types that do have handlers."""
        self.message_type = message_type
        self.handled_types = handled_types
        unhandled = f"{message_type.__module__}.{message_type.__qualname__}"
        known = ", ".join(handled_types) or "<none>"
        hint = "import the module that declares it"
        super().__init__(f"no handler registered for {unhandled}; {hint}. handled types: {known}")
