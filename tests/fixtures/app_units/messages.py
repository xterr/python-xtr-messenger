"""The messages the fixture app handles."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class Record:
    """Handled by two handlers, one of which dispatches :class:`Nested`."""

    value: str


@dataclass(frozen=True, slots=True)
class Nested:
    """Dispatched while a :class:`Record` is handled."""

    value: str
