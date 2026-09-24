"""An encoded envelope could not be turned back into a message."""

from __future__ import annotations

from .message_bus_error import MessageBusError

__all__ = ["MessageDecodingFailedError"]


class MessageDecodingFailedError(MessageBusError):
    """An encoded envelope could not be turned back into a typed message.

    Deliberately loud: a silently mis-decoded payload reaches the handler as
    the wrong shape and fails far from its cause.
    """

    reason: str
    message_name: str | None

    def __init__(self, reason: str, message_name: str | None = None) -> None:
        """Record why decoding failed, and for which message when known."""
        self.reason = reason
        self.message_name = message_name
        where = f" for {message_name!r}" if message_name is not None else ""
        super().__init__(f"cannot decode envelope{where}: {reason}")
