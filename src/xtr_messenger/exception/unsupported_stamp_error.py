"""A transport was handed a stamp it cannot honour."""

from __future__ import annotations

from .message_bus_error import MessageBusError

__all__ = ["UnsupportedStampError"]


class UnsupportedStampError(MessageBusError):
    """A transport was handed a stamp it cannot act on.

    A stamp a transport ignores is a silent failure: the caller asked for
    behaviour — a delay, say — and the message went out immediately with no
    sign the request was dropped. Raising names the stamp and the transport
    so the mismatch surfaces where it is made, not as a message that never
    behaved as asked.
    """

    stamp_name: str
    transport: str

    def __init__(self, stamp_name: str, transport: str) -> None:
        """Record which stamp could not be honoured, and by which transport."""
        self.stamp_name = stamp_name
        self.transport = transport
        super().__init__(f"the {transport!r} transport cannot honour a {stamp_name}")
