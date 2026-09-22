"""Records which delivery attempt this is."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime

from .stamp_interface import StampInterface

__all__ = ["RedeliveryStamp"]


@dataclass(frozen=True, slots=True)
class RedeliveryStamp(StampInterface):
    """Records which delivery attempt this is.

    ``retry_count`` is zero on first delivery and increments per redelivery,
    so a handler can tell a first attempt from a final one.
    """

    retry_count: int
    redelivered_at: datetime = field(default_factory=lambda: datetime.now(UTC))
