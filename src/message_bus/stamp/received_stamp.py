"""Marks an envelope as having arrived from a transport."""

from __future__ import annotations

from dataclasses import dataclass

from .non_sendable_stamp_interface import NonSendableStampInterface

__all__ = ["ReceivedStamp"]


@dataclass(frozen=True, slots=True)
class ReceivedStamp(NonSendableStampInterface):
    """Marks an envelope as having arrived from a transport.

    Its presence tells
    :class:`~message_bus.middleware.SendMessageMiddleware` not to route the
    message again, which is what stops a consumer from re-publishing
    everything it consumes.
    """

    transport_name: str
