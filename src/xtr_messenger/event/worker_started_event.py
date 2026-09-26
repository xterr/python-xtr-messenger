"""Dispatched once, when a worker starts consuming."""

from __future__ import annotations

from typing import TYPE_CHECKING, final

from xtr_event_dispatcher_contracts import Event

if TYPE_CHECKING:
    from xtr_messenger.worker_interface import WorkerInterface

__all__ = ["WorkerStartedEvent"]


@final
class WorkerStartedEvent(Event):
    """A worker started consuming."""

    def __init__(self, worker: WorkerInterface) -> None:
        """Describe ``worker`` starting."""
        self._worker = worker

    @property
    def worker(self) -> WorkerInterface:
        """Return the worker that started."""
        return self._worker
