"""A transport setting holds a value its adapter cannot use."""

from __future__ import annotations

from .message_bus_error import MessageBusError

__all__ = ["InvalidTransportOptionError"]


class InvalidTransportOptionError(MessageBusError):
    """A transport setting holds a value its adapter cannot use.

    The setting may have come from the DSN's query string or from
    ``options`` — both reach an adapter the same way, so neither is named.
    """

    option: str
    value: str
    expected: str

    def __init__(self, option: str, value: str, expected: str) -> None:
        """Record the setting, the value it held, and what it should hold."""
        self.option = option
        self.value = value
        self.expected = expected
        super().__init__(f"{option}={value!r} must be {expected}")
