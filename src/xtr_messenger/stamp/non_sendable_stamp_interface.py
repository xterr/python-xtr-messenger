"""The contract for stamps that must not cross a transport boundary."""

from __future__ import annotations

from dataclasses import dataclass

from .stamp_interface import StampInterface

__all__ = ["NonSendableStampInterface"]


@dataclass(frozen=True, slots=True)
class NonSendableStampInterface(StampInterface):
    """Marker for stamps that must never cross a transport boundary.

    Serializers drop these before encoding. Use it for markers describing the
    local process — what received this, what handled it — rather than facts
    the consumer needs.
    """
