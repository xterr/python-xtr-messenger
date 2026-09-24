"""No transport is routed for a dispatched message type."""

from __future__ import annotations

from .message_bus_error import MessageBusError

__all__ = ["NoSenderForMessageError"]


class NoSenderForMessageError(MessageBusError):
    """No transport is routed for a dispatched message type.

    Raised before any transport is touched, so nothing was enqueued when it
    surfaces.
    """

    message_type: type
    routed_types: tuple[str, ...]

    def __init__(self, message_type: type, routed_types: tuple[str, ...]) -> None:
        """Record the unrouted type alongside the types that are routed."""
        self.message_type = message_type
        self.routed_types = routed_types
        unrouted = f"{message_type.__module__}.{message_type.__qualname__}"
        routed = ", ".join(routed_types) or "<none>"
        super().__init__(f"no transport routed for {unrouted}; routed types: {routed}")
