"""What a console command was asked cannot be done as asked."""

from __future__ import annotations

from .message_bus_error import MessageBusError

__all__ = ["InvalidCommandInputError"]


class InvalidCommandInputError(MessageBusError):
    """The arguments and options a command was given cannot be done together, or do not read.

    Raised by the ``messenger:failed:*`` commands — a filter combined with
    message ids, a date that does not read — and reported to the operator.

    Attributes:
        reason: What is wrong with what was asked, as the operator reads it.
    """

    reason: str

    def __init__(self, reason: str) -> None:
        """Record what is wrong with what was asked."""
        self.reason = reason
        super().__init__(reason)
