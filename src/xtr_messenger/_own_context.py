"""Handling a received message in a context of its own.

Something runs a worker — a console command, an application's entry point —
and what that context carries is the host's, not a message's: a unit of work
open there would be joined by every message the worker handles, several at
once among them. So a message is handled in a context started for it, where it
finds only what was opened for it, and leaves nothing behind for the next.

A broker's receive loop is started the same way, in a context of its own that
says so: the task the loop starts for a message is a copy of it, so carries
nothing of the host's already, and only has to stop saying it is the loop.
Anything else — a message kicked in-process from inside a handler — reads as
not in the loop, and is given a context of its own.
"""

from __future__ import annotations

import asyncio
from contextvars import Context, ContextVar
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Coroutine

__all__ = ["in_own_context", "in_receive_loop", "leave_receive_loop", "started_in_receive_loop"]


# True in the context a receive loop is started in, and so in each task it
# starts — until that task, a message's, says it is not the loop.
_RECEIVE_LOOP: ContextVar[bool] = ContextVar("xtr_messenger_receive_loop", default=False)


async def in_own_context[T](work: Coroutine[object, object, T]) -> T:
    """Run ``work`` in a context of its own, and return what it returns.

    The context starts empty: no variable set where this is called reaches
    ``work``, and none ``work`` sets is seen once it is done. It runs as a
    task of its own, awaited here, so what it raises is raised here and
    cancelling the caller cancels it.
    """
    return await asyncio.get_running_loop().create_task(work, context=Context())


async def in_receive_loop[T](work: Coroutine[object, object, T]) -> T:
    """Run the receive loop ``work`` as :func:`in_own_context` does, in a context saying so."""
    context = Context()
    context.run(_enter_receive_loop)
    return await asyncio.get_running_loop().create_task(work, context=context)


def _enter_receive_loop() -> None:
    """Say the context this runs in is a receive loop's."""
    _ = _RECEIVE_LOOP.set(True)


def started_in_receive_loop() -> bool:
    """Tell whether this context is a receive loop's, or a copy of one not yet left.

    A message task the loop started carries nothing of whatever ran the
    worker, so needs no context started for it again — which would only drop
    what was set for it since: what a broker's middleware attached to the
    message it is about to handle.
    """
    return _RECEIVE_LOOP.get()


def leave_receive_loop() -> None:
    """Say this context — a message task's copy of the loop's — is no longer the loop.

    The copy is the task's own, so the loop goes on saying it is. What the
    message's handlers kick in-process from here then reads as not in the
    loop, and is handled in a context of its own rather than in theirs.
    """
    _ = _RECEIVE_LOOP.set(False)
