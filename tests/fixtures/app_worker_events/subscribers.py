"""A subscriber recording the worker events it hears."""

from __future__ import annotations

from typing import TYPE_CHECKING

from typing_extensions import override
from xtr_event_dispatcher import EventSubscriberInterface

from xtr_messenger.event import (
    WorkerMessageHandledEvent,
    WorkerMessageReceivedEvent,
    WorkerStartedEvent,
    WorkerStoppedEvent,
)

if TYPE_CHECKING:
    from collections.abc import Mapping

    from xtr_event_dispatcher import SubscribedEvents


class WorkerJournal(EventSubscriberInterface):
    """Writes down each worker event, by name."""

    def __init__(self) -> None:
        """Start with nothing written down."""
        self.entries: list[str] = []

    @classmethod
    @override
    def get_subscribed_events(cls) -> Mapping[str | type, SubscribedEvents]:
        return {
            WorkerStartedEvent: "record",
            WorkerMessageReceivedEvent: "record",
            WorkerMessageHandledEvent: "record",
            WorkerStoppedEvent: "record",
        }

    def record(self, event: object) -> None:
        """Write down that ``event`` happened."""
        self.entries.append(type(event).__name__)
