"""Dispatched each time a worker has settled a message."""

from __future__ import annotations

from typing import TYPE_CHECKING, final

from xtr_event_dispatcher_contracts import Event

if TYPE_CHECKING:
    from xtr_messenger.worker_interface import WorkerInterface

__all__ = ["WorkerRunningEvent"]


@final
class WorkerRunningEvent(Event):
    """A worker settled a message and is going on.

    The place for a listener that stops a worker after so many messages, or
    once memory runs high: calling ``worker.stop()`` here ends the run before
    the next message is collected.

    Transports here push messages rather than being polled, so a worker has no
    moment at which it knows it is idle; :attr:`is_idle` is ``False`` for every
    event the library dispatches today.
    """

    def __init__(self, worker: WorkerInterface, *, is_idle: bool = False) -> None:
        """Describe ``worker`` running, idle or not."""
        self._worker = worker
        self._is_idle = is_idle

    @property
    def worker(self) -> WorkerInterface:
        """Return the worker that is running."""
        return self._worker

    @property
    def is_idle(self) -> bool:
        """Tell whether the worker had nothing to do."""
        return self._is_idle
