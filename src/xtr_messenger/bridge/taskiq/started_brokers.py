"""The publish-side connection lifecycle: open once, per broker, per owner.

Constructing a broker performs no I/O — a connection is only opened by
``startup()``. That is why an application can safely build its broker at
module level, which is also what the taskiq worker CLI needs when it imports a
``module:attribute`` path. The cost is that a *producer* must open the
connection itself before its first publish, and must close it again when it is
done, because the consuming side's run loop is what normally does both and a
publisher has no loop.

That bookkeeping is held per owner rather than per process. One process can run
two applications — two kernels, two test cases — each with its own brokers, and
an owner that closed every started broker in the process would close
connections the other is still publishing through.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, final
from weakref import WeakKeyDictionary, WeakSet

if TYPE_CHECKING:
    from taskiq import AsyncBroker

__all__ = ["StartedBrokers"]


@final
class StartedBrokers:
    """The brokers one owner opened for publishing.

    Build one per owner — a transport factory, say — and hand it to every
    sender that owner builds, so senders sharing a broker open it once between
    them. Mutable by design: tracking what is open is the whole point.
    """

    __slots__ = ("_locks", "_started")

    def __init__(self) -> None:
        """Start tracking nothing."""
        # A weak set so the entry dies with the broker — keying by ``id()``
        # meant a garbage-collected broker could leave its address behind for a
        # new one to inherit, which then skipped its own startup and failed on
        # first publish.
        self._started: WeakSet[AsyncBroker] = WeakSet()
        # One lock per event loop. A single lock binds to whichever loop first
        # awaits it, which breaks a second ``asyncio.run`` in the same process
        # — two test cases, or a worker restarted after shutdown.
        self._locks: WeakKeyDictionary[asyncio.AbstractEventLoop, asyncio.Lock] = (
            WeakKeyDictionary()
        )

    async def ensure_started(self, broker: AsyncBroker) -> None:
        """Open ``broker``'s connection once, for publishing.

        Publishing from a process that never called ``startup()`` fails, so a
        producer has to open the connection itself. Three things matter here:

        * In a worker the receiver has already started the broker. Starting it
          again re-fires the worker startup events, duplicating whatever they
          set up, so that check comes first.
        * The record is per broker instance, so an owner with two brokers
          starts each of them.
        * The lock makes concurrent first publishes safe.
        """
        if broker.is_worker_process or broker in self._started:
            return
        async with self._lock_for_this_loop():
            if broker in self._started:
                return
            await broker.startup()
            self._started.add(broker)

    def forget(self, broker: AsyncBroker) -> None:
        """Drop the record for ``broker``, so a later publish opens it again."""
        self._started.discard(broker)

    async def close_all(self) -> None:
        """Shut down every broker opened through :meth:`ensure_started`.

        Each broker is forgotten as it closes, so a later publish in the same
        process opens it again — and a broker whose shutdown raised is
        forgotten too, so the next ``close_all`` does not try it a second time.

        A broker a worker owns was never recorded here, so this never closes a
        connection something is still consuming from — and neither does it
        touch another owner's brokers.

        Raises:
            ExceptionGroup: If one or more shutdowns raised. Every broker is
                tried first; the failures are collected and re-raised together,
                so one broker's failure never leaves the rest open.
        """
        failures: list[Exception] = []
        for broker in tuple(self._started):
            self._started.discard(broker)
            try:
                await broker.shutdown()
            except Exception as error:  # noqa: BLE001 — every broker is tried; failures are re-raised together below.
                failures.append(error)
        if failures:
            raise ExceptionGroup("closing publisher brokers failed", failures)

    def _lock_for_this_loop(self) -> asyncio.Lock:
        loop = asyncio.get_running_loop()
        lock = self._locks.get(loop)
        if lock is None:
            lock = asyncio.Lock()
            self._locks[loop] = lock
        return lock
