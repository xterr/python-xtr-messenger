"""Dispatched when a worker has collected a message and is about to handle it."""

from __future__ import annotations

from typing import TYPE_CHECKING, final

from .abstract_worker_message_event import AbstractWorkerMessageEvent

if TYPE_CHECKING:
    from xtr_messenger.envelope import Envelope

__all__ = ["WorkerMessageReceivedEvent"]


@final
class WorkerMessageReceivedEvent(AbstractWorkerMessageEvent):
    """A message was collected and is about to be handled.

    A listener may decide it should not be handled at all. The worker then
    acknowledges it without dispatching it: skipping is a decision, not a
    failure, and rejecting would send it wherever failed messages go.
    """

    def __init__(self, envelope: Envelope, receiver_name: str) -> None:
        """Describe ``envelope`` from ``receiver_name``, to be handled unless told not to."""
        super().__init__(envelope, receiver_name)
        self._should_handle = True

    def should_handle(self, value: bool | None = None) -> bool:
        """Return whether the message will be handled, first setting it when ``value`` is given."""
        if value is not None:
            self._should_handle = value
        return self._should_handle
