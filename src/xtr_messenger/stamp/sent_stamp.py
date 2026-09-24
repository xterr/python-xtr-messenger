"""Records that a sender accepted the envelope."""

from __future__ import annotations

from dataclasses import dataclass

from .non_sendable_stamp_interface import NonSendableStampInterface

__all__ = ["SentStamp"]


@dataclass(frozen=True, slots=True)
class SentStamp(NonSendableStampInterface):
    """Records that a sender accepted the envelope.

    One stamp per sender the message was routed to, appended by
    :class:`~xtr_messenger.middleware.SendMessageMiddleware`.
    """

    sender_class: str
    sender_alias: str
