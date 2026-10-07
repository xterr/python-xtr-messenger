"""Running taskiq's own worker behind this library's worker contract."""

from __future__ import annotations

import asyncio
from os import cpu_count
from typing import TYPE_CHECKING, Final, final

from typing_extensions import override

from xtr_messenger.event import WorkerStartedEvent, WorkerStoppedEvent
from xtr_messenger.worker_interface import WorkerInterface

from .poison_message_receiver import PoisonMessageReceiver

if TYPE_CHECKING:
    from taskiq import AsyncBroker
    from xtr_event_dispatcher_contracts import EventDispatcherInterface

    from .poison_message_receiver import DeadLetterer
    from .started_brokers import StartedBrokers

__all__ = ["TaskiqWorker", "default_max_async_tasks"]

_TASKS_PER_CPU: Final = 10
_MAX_DEFAULT_TASKS: Final = 100


def default_max_async_tasks() -> int:
    """Return how many messages a worker handles at once, unless told otherwise.

    Ten per CPU, capped at 100 — taskiq's own CLI default. The handlers share
    one event loop, so the CPUs stand in for the size of the machine rather
    than bound the concurrency; what bounds it in practice is what the
    handlers wait on, a database pool say, which is why it is configurable.
    """
    return min(_MAX_DEFAULT_TASKS, _TASKS_PER_CPU * (cpu_count() or 1))


@final
class TaskiqWorker(WorkerInterface):
    """Consumes with taskiq's worker, presented as a :class:`WorkerInterface`.

    Nothing here reimplements consuming. taskiq already has a worker that
    handles prefetch, concurrency limits, acknowledgement timing and retry,
    and reproducing that would be a large amount of subtly wrong code. So it
    is wrapped rather than replaced, and the only thing this adds is the
    library's own face.

    That face is the point. A worker entrypoint holds a
    :class:`~xtr_messenger.worker_interface.WorkerInterface` and calls ``run()``; it
    never imports taskiq, never names a broker, and does not change if this
    transport is swapped for one that brings no worker of its own. The
    dependency stops here.

    Running this in-process also replaces the ``taskiq worker`` CLI, so a
    worker is an ordinary Python entrypoint that can set up logging, open
    connections, and read configuration the same way the rest of the
    application does.
    """

    __slots__ = (
        "_broker",
        "_dead_letter",
        "_dispatcher",
        "_finished",
        "_max_async_tasks",
        "_max_prefetch",
        "_publishers",
        "_stop_requested",
    )

    def __init__(  # noqa: PLR0913 — everything past `max_prefetch` is keyword-only
        self,
        broker: AsyncBroker,
        max_async_tasks: int | None = None,
        max_prefetch: int = 0,
        *,
        event_dispatcher: EventDispatcherInterface | None = None,
        publishers: StartedBrokers | None = None,
        dead_letter: DeadLetterer | None = None,
    ) -> None:
        """Consume from ``broker``, handling at most ``max_async_tasks`` at once.

        ``None`` is :func:`default_max_async_tasks` — never unbounded, which
        taskiq warns can end in undefined behaviour.

        ``event_dispatcher`` hears this worker start and stop. The events
        about each message come from the tasks
        :func:`~xtr_messenger.bridge.taskiq.binding.bind_bus` registers; a
        running event after each message is not dispatched, since taskiq
        runs messages concurrently and reports none of them back here.

        ``publishers`` is the registry a producer in this process records
        ``broker`` in, when one shares it. This worker shuts the broker down as
        it stops, so it forgets it there too and the next publish opens it
        again. Leave it out when nothing publishes through this broker here.

        ``dead_letter`` is where a poison delivery — unparsable, or naming a
        task nobody registered — is quarantined; it is then acknowledged
        rather than redelivered forever. Without it such a delivery is
        acknowledged and dropped.
        """
        self._dispatcher = event_dispatcher
        self._broker = broker
        self._publishers = publishers
        self._dead_letter = dead_letter
        self._max_async_tasks = (
            max_async_tasks if max_async_tasks is not None else default_max_async_tasks()
        )
        self._max_prefetch = max_prefetch
        self._finished: asyncio.Event | None = None
        self._stop_requested = False

    @property
    def max_async_tasks(self) -> int:
        """Return how many messages this worker handles at once, at most."""
        return self._max_async_tasks

    @property
    def broker(self) -> AsyncBroker:
        """Return the broker being consumed from.

        For inspecting what was wired — which queues are declared, whether a
        connection is open. Callers outside this package should depend on
        :class:`~xtr_messenger.worker_interface.WorkerInterface` instead, which is what
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

        The broker is shut down on the way out too — taskiq's receiver starts
        it and leaves closing it to taskiq's CLI, which this replaces. Shut
        down while still flagged as a worker, so the worker shutdown events
        fire to match the worker startup events; and forgotten in the
        ``publishers`` registry, if one was given, so a producing side sharing
        it opens it again on its next publish.
        """
        claimed = self._broker.is_worker_process
        self._broker.is_worker_process = True
        self._finished = asyncio.Event()
        if self._stop_requested:
            self._stop_requested = False
            self._finished.set()
        receiver = PoisonMessageReceiver(
            self._broker,
            dead_letter=self._dead_letter,
            max_async_tasks=self._max_async_tasks,
            max_prefetch=self._max_prefetch,
            run_startup=True,
        )
        try:
            if self._dispatcher is not None:
                _ = await self._dispatcher.dispatch(WorkerStartedEvent(self))
            await receiver.listen(self._finished)
        finally:
            try:
                await self._broker.shutdown()
            finally:
                if self._publishers is not None:
                    self._publishers.forget(self._broker)
                self._broker.is_worker_process = claimed
                self._finished = None
                if self._dispatcher is not None:
                    _ = await self._dispatcher.dispatch(WorkerStoppedEvent(self))

    @override
    def stop(self) -> None:
        """Ask the worker to finish what it has and return.

        Asked while not running — before :meth:`run` has started, say — the
        next run returns without consuming anything.
        """
        if self._finished is not None:
            self._finished.set()
        else:
            self._stop_requested = True
