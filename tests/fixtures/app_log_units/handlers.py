"""A message whose handler notes what stands for the log unit it is handled in."""

from __future__ import annotations

from dataclasses import dataclass

from xtr_logging.log_unit import unit_state

from xtr_messenger import as_message, as_message_handler

__all__ = ["MARK", "UNITS", "Logged", "log_unit_of_this", "note_the_log_unit"]

MARK = object()
"""The owner under which each log unit keeps the object that stands for it."""

UNITS: list[object | None] = []
"""What stood for the log unit each message was handled in."""


def log_unit_of_this() -> object | None:
    """Return what stands for the log unit open here, ``None`` outside one."""
    return unit_state(MARK, object)


@as_message(name="tests.fixtures.app_log_units.logged.v1")
@dataclass(frozen=True, slots=True)
class Logged:
    """A message whose handler notes its log unit."""

    value: str


@as_message_handler(Logged)
async def note_the_log_unit(message: Logged) -> None:
    """Note the log unit this message is handled in."""
    del message
    UNITS.append(log_unit_of_this())
