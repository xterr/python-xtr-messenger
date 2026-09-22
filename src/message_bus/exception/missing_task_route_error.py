"""A message name has nothing registered to consume it."""

from __future__ import annotations

from .message_bus_error import MessageBusError

__all__ = ["MissingTaskRouteError"]


class MissingTaskRouteError(MessageBusError):
    """A message type has no task registered under its name on the broker.

    Publishing does not fail for an unknown name — the broker accepts the
    message and nothing ever consumes it. Checking at startup turns that
    silent loss into a boot failure.
    """

    missing: tuple[str, ...]
    registered: tuple[str, ...]

    def __init__(self, missing: tuple[str, ...], registered: tuple[str, ...]) -> None:
        """Record the unconsumable names alongside what the broker does know."""
        self.missing = missing
        self.registered = registered
        known = ", ".join(sorted(registered)) or "<none>"
        names = ", ".join(missing)
        remedy = f"Import the handler module(s) first. Registered: {known}"
        super().__init__(f"no task registered for message name(s): {names}. {remedy}")
