"""A transport DSN carries an option its adapter does not recognise."""

from __future__ import annotations

from .message_bus_error import MessageBusError

__all__ = ["UnknownTransportOptionError"]


class UnknownTransportOptionError(MessageBusError):
    """A transport DSN carries an option its adapter does not recognise.

    Raised rather than ignored because an unrecognised option is always a
    mistake in configuration someone wrote by hand — a typo, or a setting
    meant for a different scheme. Dropping it silently leaves the transport
    running on defaults that nobody chose, which is the failure this exists
    to prevent.
    """

    scheme: str
    unknown: tuple[str, ...]
    known: tuple[str, ...]

    def __init__(self, scheme: str, unknown: tuple[str, ...], known: tuple[str, ...]) -> None:
        """Record the scheme, what it did not recognise, and what it accepts."""
        self.scheme = scheme
        self.unknown = unknown
        self.known = known
        offending = ", ".join(unknown)
        accepted = ", ".join(known) or "<none>"
        super().__init__(
            f"{scheme}:// does not accept {offending}; it accepts: {accepted}",
        )
