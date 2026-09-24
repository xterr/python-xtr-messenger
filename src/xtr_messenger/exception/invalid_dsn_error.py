"""A DSN is missing the scheme that selects its transport."""

from __future__ import annotations

from .message_bus_error import MessageBusError

__all__ = ["InvalidDsnError"]


class InvalidDsnError(MessageBusError):
    """A DSN is missing the scheme that selects its transport."""

    dsn: str

    def __init__(self, dsn: str) -> None:
        """Record the DSN that could not be read."""
        self.dsn = dsn
        super().__init__(f"dsn {dsn!r} has no scheme; expected something like 'amqp://host'")
