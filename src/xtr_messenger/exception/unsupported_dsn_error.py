"""No factory recognised a transport's DSN."""

from __future__ import annotations

from xtr_messenger._redaction import redacted

from .message_bus_error import MessageBusError

__all__ = ["UnsupportedDsnError"]


class UnsupportedDsnError(MessageBusError):
    """No factory recognised a transport's DSN."""

    transport_name: str
    dsn: str

    def __init__(self, transport_name: str, dsn: str) -> None:
        """Record which transport could not be built, and from what."""
        self.transport_name = transport_name
        self.dsn = dsn
        remedy = "install the matching extra, or pass your own factory"
        shown = redacted(dsn)
        super().__init__(f"no transport factory for {transport_name!r} dsn {shown!r}; {remedy}")
