"""Requests delayed delivery, in milliseconds."""

from __future__ import annotations

from dataclasses import dataclass

from .stamp_interface import StampInterface

__all__ = ["DelayStamp"]


@dataclass(frozen=True, slots=True)
class DelayStamp(StampInterface):
    """Requests delayed delivery, in milliseconds."""

    delay_ms: int
