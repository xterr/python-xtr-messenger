"""The application's message, and its handler."""

from __future__ import annotations

from dataclasses import dataclass

from xtr_dependency_injection import as_service

from xtr_messenger import as_message, as_message_handler


@as_message(name="tests.receivers.tick.v1")
@dataclass(frozen=True)
class Tick:
    """Something the registered receiver produces."""

    number: int


@as_service
class Ticks:
    """What the handler saw."""

    def __init__(self) -> None:
        """Start with nothing seen."""
        self.seen: list[int] = []


@as_message_handler(Tick)
class CountTicks:
    """Writes each tick down."""

    def __init__(self, ticks: Ticks) -> None:
        """Write into ``ticks``."""
        self._ticks: Ticks = ticks

    async def __call__(self, message: Tick) -> None:
        """Record the tick."""
        self._ticks.seen.append(message.number)
