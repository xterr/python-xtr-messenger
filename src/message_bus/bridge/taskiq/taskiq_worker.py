"""Running taskiq's own worker behind this library's worker contract."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, final

from taskiq.receiver import Receiver
from typing_extensions import override

from message_bus.worker_interface import WorkerInterface

if TYPE_CHECKING:
    from taskiq import AsyncBroker

__all__ = ["TaskiqWorker"]


@final
class TaskiqWorker(WorkerInterface):
    """Consumes with taskiq's worker, presented as a :class:`WorkerInterface`.

    Nothing here reimplements consuming. taskiq already has a worker that
    handles prefetch, concurrency limits, acknowledgement timing and retry,
    and reproducing that would be a large amount of subtly wrong code. So it
    is wrapped rather than replaced, and the only thing this adds is the
    library's own face.

    That face is the point. A worker entrypoint holds a
    :class:`~message_bus.worker_interface.WorkerInterface` and calls ``run()``; it
    never imports taskiq, never names a broker, and does not change if this
    transport is swapped for one that brings no worker of its own. The
    dependency stops here.

    Running this in-process also replaces the ``taskiq worker`` CLI, so a
    worker is an ordinary Python entrypoint that can set up logging, open
    connections, and read configuration the same way the rest of the
    application does.
    """

    __slots__ = ("_broker", "_finished", "_max_async_tasks", "_max_prefetch")

    def __init__(
        self,
        broker: AsyncBroker,
        max_async_tasks: int | None = None,
        max_prefetch: int = 0,
    ) -> None:
        """Consume from ``broker``, bounded by the given concurrency limits."""
        self._broker = broker
        self._max_async_tasks = max_async_tasks
        self._max_prefetch = max_prefetch
        self._finished: asyncio.Event | None = None

    @property
    def broker(self) -> AsyncBroker:
        """Return the broker being consumed from.

        For inspecting what was wired — which queues are declared, whether a
        connection is open. Callers outside this package should depend on
        :class:`~message_bus.worker_interface.WorkerInterface` instead, which is what
        keeps them free of taskiq.
        """
        return self._broker

    @override
    async def run(self) -> None:
        """Consume until :meth:`stop` is called or the task is cancelled.

        Marks the broker as belonging to a worker while running. That flag
        is what stops a publish from this same process opening the connection
        a second time and re-firing the startup events the receiver has
        already run — a duplicate that is invisible until something
        registered twice fires twice.

        The flag is restored on the way out. It lives on the broker, which
        the producing side may share, so leaving it set would tell every
        later publish that a worker owns a connection nothing is consuming.
        """
        claimed = self._broker.is_worker_process
        self._broker.is_worker_process = True
        self._finished = asyncio.Event()
        receiver = Receiver(
            self._broker,
            max_async_tasks=self._max_async_tasks,
            max_prefetch=self._max_prefetch,
            run_startup=True,
        )
        try:
            await receiver.listen(self._finished)
        finally:
            self._broker.is_worker_process = claimed
            self._finished = None

    def stop(self) -> None:
        """Ask a running worker to finish what it has and return.

        Safe to call before :meth:`run`, or after it returns: a worker that
        is not running has nothing to wind down.
        """
        if self._finished is not None:
            self._finished.set()
