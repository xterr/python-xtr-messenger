"""A handler class the container cannot build."""

from __future__ import annotations

from .message_bus_error import MessageBusError

__all__ = ["UnresolvableHandlerError"]


class UnresolvableHandlerError(MessageBusError):
    """A handler class the container cannot build, or that cannot be called.

    Raised when a class was bound as a handler but the container returns
    nothing for it — usually because it was never registered as an
    injectable — or when it has no ``__call__`` to invoke.
    """

    handler_type: type

    def __init__(self, handler_type: type) -> None:
        """Record the class that could not be used as a handler."""
        self.handler_type = handler_type
        remedy = "register it as an injectable and give it an async __call__"
        super().__init__(f"cannot use {handler_type.__qualname__} as a handler; {remedy}")
