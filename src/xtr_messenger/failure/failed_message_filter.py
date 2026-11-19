"""Selecting failed messages by class and by when they failed."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, final

from xtr_messenger._text import class_name
from xtr_messenger.stamp import SentToFailureTransportStamp

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from xtr_messenger.envelope import Envelope
    from xtr_messenger.transport.receiver.listable_receiver_interface import (
        ListableReceiverInterface,
    )

__all__ = ["FailedMessageFilter"]


@final
@dataclass(frozen=True, slots=True)
class FailedMessageFilter:
    """Selects failed messages by class name and by the time they failed.

    Both bounds of the time window are inclusive. The time a message failed is
    the one its :class:`~xtr_messenger.stamp.SentToFailureTransportStamp`
    records, so a message carrying none is never selected by a time window.

    Attributes:
        class_name: The dotted name of the message's class —
            ``module.QualName`` — to select only those.
        failed_after: Select only messages that failed at or after this.
        failed_before: Select only messages that failed at or before this.
    """

    class_name: str | None = None
    failed_after: datetime | None = None
    failed_before: datetime | None = None

    def is_empty(self) -> bool:
        """Tell whether this selects every message."""
        return self.class_name is None and self.failed_after is None and self.failed_before is None

    def matches(self, envelope: Envelope) -> bool:
        """Tell whether ``envelope`` is one this selects."""
        if self.class_name is not None and self.class_name != class_name(envelope.message):
            return False
        if self.failed_after is None and self.failed_before is None:
            return True
        stamp = envelope.last(SentToFailureTransportStamp)
        if stamp is None:
            return False
        failed_at = stamp.failed_at
        return (self.failed_after is None or failed_at >= self.failed_after) and (
            self.failed_before is None or failed_at <= self.failed_before
        )

    async def select(
        self, receiver: ListableReceiverInterface, limit: int | None = None
    ) -> AsyncIterator[Envelope]:
        """Yield what ``receiver`` keeps that this selects, ``limit`` of them at most.

        The limit bounds what is selected: when this selects some messages
        only, every one is read until that many match.
        """
        if self.is_empty():
            async for envelope in receiver.all(limit):
                yield envelope
            return
        if limit is not None and limit < 1:
            return
        matched = 0
        async for envelope in receiver.all():
            if not self.matches(envelope):
                continue
            yield envelope
            matched += 1
            if limit is not None and matched >= limit:
                return
