"""Marks a message moved to a failure transport once its attempts ran out."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from .stamp_interface import StampInterface

__all__ = ["SentToFailureTransportStamp"]


@dataclass(frozen=True, slots=True)
class SentToFailureTransportStamp(StampInterface):
    """Where a message failed for good, and when.

    Added by the transport that moves a message to its failure transport, so
    the ``messenger:failed:*`` commands can say which transport it failed on
    and select it by when it failed.

    Attributes:
        original_receiver_name: The transport the message failed on.
        failed_at: When it was moved to the failure transport, timezone-aware.
    """

    original_receiver_name: str
    failed_at: datetime
