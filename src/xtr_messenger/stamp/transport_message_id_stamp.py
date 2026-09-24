"""The transport-assigned identifier for an enqueued message."""

from __future__ import annotations

from dataclasses import dataclass

from .stamp_interface import StampInterface

__all__ = ["TransportMessageIdStamp"]


@dataclass(frozen=True, slots=True)
class TransportMessageIdStamp(StampInterface):
    """The transport-assigned identifier for an enqueued message."""

    message_id: str
