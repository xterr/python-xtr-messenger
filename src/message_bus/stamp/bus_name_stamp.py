"""Names the bus a message was dispatched on."""

from __future__ import annotations

from dataclasses import dataclass

from .stamp_interface import StampInterface

__all__ = ["BusNameStamp"]


@dataclass(frozen=True, slots=True)
class BusNameStamp(StampInterface):
    """Names the bus a message was dispatched on."""

    bus_name: str
