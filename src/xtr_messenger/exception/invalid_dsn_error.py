"""A DSN that cannot be read into a transport selection."""

from __future__ import annotations

from xtr_messenger._redaction import redacted

from .message_bus_error import MessageBusError

__all__ = ["InvalidDsnError"]


class InvalidDsnError(MessageBusError):
    """A DSN that cannot be read into a transport selection.

    The message shows the DSN redacted, never as written — an invalid DSN is
    exactly the kind whose credentials end up somewhere unexpected.
    """

    dsn: str
    reason: str

    def __init__(self, dsn: str, reason: str | None = None) -> None:
        """Record the DSN that could not be read, and why.

        Without ``reason``, the DSN is missing the scheme that selects its
        transport — the commonest way one goes wrong.
        """
        self.dsn = dsn
        self.reason = (
            reason if reason is not None else "has no scheme; expected something like 'amqp://host'"
        )
        shown = redacted(dsn)
        super().__init__(f"dsn {shown!r} {self.reason}")
