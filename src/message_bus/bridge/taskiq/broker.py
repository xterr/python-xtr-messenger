"""Broker construction and connection lifecycle.

Constructing a broker performs no I/O — a connection is only opened by
``startup()``. That is why an application can safely build its broker at
module level, which is also what the taskiq worker CLI needs when it
imports a ``module:attribute`` path.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING
from weakref import WeakKeyDictionary, WeakSet

if TYPE_CHECKING:
    from taskiq import AsyncBroker

__all__ = ["ensure_started", "forget_started"]

#: Brokers already opened for publishing. A weak set so the entry dies with
#: the broker — keying by ``id()`` meant a garbage-collected broker could
#: leave its address behind for a new one to inherit, which then skipped its
#: own startup and failed on first publish.
_started: WeakSet[AsyncBroker] = WeakSet()

#: One lock per event loop. A single module-level lock binds to whichever
#: loop first awaits it, which breaks a second ``asyncio.run`` in the same
#: process — two test cases, or a worker restarted after shutdown.
_locks: WeakKeyDictionary[asyncio.AbstractEventLoop, asyncio.Lock] = WeakKeyDictionary()


def _lock_for_this_loop() -> asyncio.Lock:
    loop = asyncio.get_running_loop()
    lock = _locks.get(loop)
    if lock is None:
        lock = asyncio.Lock()
        _locks[loop] = lock
    return lock


async def ensure_started(broker: AsyncBroker) -> None:
    """Open the broker's connection once, for publishing.

    Publishing from a process that never called ``startup()`` fails, so a
    producer has to open the connection itself. Three things matter here:

    * In a worker the receiver has already started the broker. Starting it
      again re-fires the worker startup events, duplicating whatever they
      set up, so that check comes first.
    * The flag is per broker instance, so an application with two brokers
      starts each of them.
    * The lock makes concurrent first publishes safe.
    """
    if broker.is_worker_process or broker in _started:
        return
    async with _lock_for_this_loop():
        if broker in _started:
            return
        await broker.startup()
        _started.add(broker)


def forget_started(broker: AsyncBroker) -> None:
    """Drop the started flag for ``broker`` — for tests."""
    _started.discard(broker)
