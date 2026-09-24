"""A handler class the container was never given."""

from __future__ import annotations

from .message_bus_error import MessageBusError

__all__ = ["UnregisteredHandlerError"]


class UnregisteredHandlerError(MessageBusError):
    """A handler class the container was never given.

    Handler classes are registered with a container as it is built, so one
    declared afterwards — its module imported too late — has nothing to build
    it from.
    """

    handler_name: str

    def __init__(self, handler_name: str) -> None:
        """Record the handler class that has no registration."""
        self.handler_name = handler_name
        hint = "import the module declaring it before building the container"
        super().__init__(f"{handler_name} was declared after the container was built; {hint}")
