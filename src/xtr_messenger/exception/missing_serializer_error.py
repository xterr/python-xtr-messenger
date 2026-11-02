"""A transport has to write its messages, and was given nothing to write them with."""

from __future__ import annotations

from .message_bus_error import MessageBusError

__all__ = ["MissingSerializerError"]


class MissingSerializerError(MessageBusError):
    """A transport has to write its messages, and was given nothing to write them with.

    Raised where the transport is built, never when the application boots: a
    process speaking only ``sync://`` has no messages to write and needs no
    serializer, so its absence only matters once a transport would use it.
    """

    scheme: str
    reason: str

    def __init__(self, scheme: str, reason: str) -> None:
        """Record which scheme needed the serializer, and where to give it one."""
        self.scheme = scheme
        self.reason = reason
        super().__init__(f"the {scheme!r} transport needs a serializer: {reason}")
