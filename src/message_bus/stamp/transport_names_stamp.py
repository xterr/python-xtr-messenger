"""Overrides routing for one dispatch, bypassing the routing table."""

from __future__ import annotations

from dataclasses import dataclass

from .stamp_interface import StampInterface

__all__ = ["TransportNamesStamp"]


@dataclass(frozen=True, slots=True)
class TransportNamesStamp(StampInterface):
    """Overrides routing for one dispatch, bypassing the routing table."""

    transport_names: tuple[str, ...]
