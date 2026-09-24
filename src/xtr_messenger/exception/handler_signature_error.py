"""A handler's parameters are not a shape the bus can call."""

from __future__ import annotations

from .message_bus_error import MessageBusError

__all__ = ["HandlerSignatureError"]


class HandlerSignatureError(MessageBusError):
    """A handler's parameters are not a shape the bus can call."""

    handler_name: str
    parameters: tuple[str, ...]

    def __init__(self, handler_name: str, parameters: tuple[str, ...]) -> None:
        """Record the handler and the parameters it declared."""
        self.handler_name = handler_name
        self.parameters = parameters
        declared = ", ".join(parameters) or "<none>"
        expected = "the message, optionally followed by one parameter annotated Envelope"
        super().__init__(f"{handler_name} declares ({declared}); a handler takes {expected}")
