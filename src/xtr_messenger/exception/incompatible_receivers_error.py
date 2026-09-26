"""One worker was asked to consume transports that cannot share a loop."""

from __future__ import annotations

from .message_bus_error import MessageBusError

__all__ = ["IncompatibleReceiversError"]


class IncompatibleReceiversError(MessageBusError):
    """A transport that brings its own worker was asked to share it with receivers.

    A broker that runs its own consume loop — taskiq does — cannot also
    drive a receiver registered with the application, and the library's own
    loop cannot drive the broker. Run them as separate workers.
    """

    brokered: tuple[str, ...]
    registered: tuple[str, ...]

    def __init__(self, brokered: tuple[str, ...], registered: tuple[str, ...]) -> None:
        """Record the transports with their own worker, and the receivers asked alongside."""
        self.brokered = brokered
        self.registered = registered
        remedy = "the first bring their own worker; consume them in separate workers"
        joined = f"{', '.join(brokered)} together with {', '.join(registered)}"
        super().__init__(f"cannot consume {joined}: {remedy}")
