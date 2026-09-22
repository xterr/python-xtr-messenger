"""Records that a handler ran, and what it returned."""

from __future__ import annotations

from dataclasses import dataclass

from .non_sendable_stamp_interface import NonSendableStampInterface

__all__ = ["HandledStamp"]


@dataclass(frozen=True, slots=True)
class HandledStamp(NonSendableStampInterface):
    """Records that a handler ran, and what it returned."""

    handler_name: str
    result: str | None = None
