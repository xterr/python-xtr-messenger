"""A message could not be turned into something a transport can carry."""

from __future__ import annotations

from .message_bus_error import MessageBusError

__all__ = ["MessageEncodingFailedError"]


class MessageEncodingFailedError(MessageBusError):
    """A message could not be turned into something a transport can carry.

    Raised on the producer's side, before anything is sent — a message type
    no codec handles, or a field with no JSON form.
    """

    reason: str
    message_name: str

    def __init__(self, reason: str, message_name: str) -> None:
        """Record why encoding failed, and for which message."""
        self.reason = reason
        self.message_name = message_name
        super().__init__(f"cannot encode {message_name!r}: {reason}")
