"""Handling a received message in a context of its own.

Something runs a worker — a console command, an application's entry point —
and what that context carries is the host's, not a message's: a unit of work
open there would be joined by every message the worker handles, several at
once among them. So a message is handled in a context started for it, where it
finds only what was opened for it, and leaves nothing behind for the next.
"""

from __future__ import annotations

import asyncio
from contextvars import Context, ContextVar
from typing import TYPE_CHECKING, TypeVar

if TYPE_CHECKING:
    from collections.abc import Coroutine

__all__ = ["in_own_context", "started_in_own_context"]

_T = TypeVar("_T")

# True in every context started here, and so in each one copied from it — the
# task a broker's own receive loop starts for a message, say.
_STARTED: ContextVar[bool] = ContextVar("xtr_messenger_own_context", default=False)


async def in_own_context(work: Coroutine[object, object, _T]) -> _T:
    """Run ``work`` in a context of its own, and return what it returns.

    The context starts empty: no variable set where this is called reaches
    ``work``, and none ``work`` sets is seen once it is done. It runs as a
    task of its own, awaited here, so what it raises is raised here and
    cancelling the caller cancels it.
    """
    return await asyncio.get_running_loop().create_task(_started(work), context=Context())


async def _started(work: Coroutine[object, object, _T]) -> _T:
    """Mark the context this runs in as started for ``work``, then run it."""
    _ = _STARTED.set(True)
    return await work


def started_in_own_context() -> bool:
    """Tell whether this context is one :func:`in_own_context` started, or a copy of one.

    Such a context carries nothing of whatever ran the worker, so work found
    in it needs no context started for it again — which would only drop what
    was set for that work since: what a broker's middleware attached to the
    message its task is about to handle.
    """
    return _STARTED.get()
