"""The messages the fixture app handles."""

from __future__ import annotations

from dataclasses import dataclass

from xtr_messenger import as_message


@dataclass(frozen=True, slots=True)
class Record:
    """Handled by two handlers, one of which dispatches :class:`Nested`."""

    value: str


@dataclass(frozen=True, slots=True)
class Nested:
    """Dispatched while a :class:`Record` is handled."""

    value: str


@as_message(name="tests.units.queued.v1")
@dataclass(frozen=True, slots=True)
class Queued:
    """Routed to the ``jobs`` transport; its handler holds a :class:`FollowUp` back."""

    value: str


@dataclass(frozen=True, slots=True)
class FollowUp:
    """Dispatched once the :class:`Queued` whose handler asked for it was handled."""

    value: str
