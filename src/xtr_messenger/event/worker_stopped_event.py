"""Dispatched once, when a worker stops consuming."""

from __future__ import annotations

from typing import TYPE_CHECKING, final

from xtr_event_dispatcher_contracts import Event

if TYPE_CHECKING:
    from xtr_messenger.worker_interface import WorkerInterface

__all__ = ["WorkerStoppedEvent"]


@final
class WorkerStoppedEvent(Event):
    """A worker stopped consuming, however it came to stop."""

    def __init__(self, worker: WorkerInterface) -> None:
        """Describe ``worker`` stopping."""
        self._worker = worker

    @property
    def worker(self) -> WorkerInterface:
        """Return the worker that stopped."""
        return self._worker
