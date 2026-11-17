"""Starting work in a context of its own."""

from __future__ import annotations

import asyncio
from contextvars import ContextVar

import pytest

from xtr_messenger._own_context import (
    in_own_context,
    in_receive_loop,
    leave_receive_loop,
    started_in_receive_loop,
)

pytestmark = pytest.mark.anyio

_TIMEOUT = 5.0
_TENANT: ContextVar[str] = ContextVar("test_own_context_tenant", default="none")


async def _tenant() -> str:
    return _TENANT.get()


async def test_the_work_does_not_see_what_the_caller_s_context_carries() -> None:
    token = _TENANT.set("caller")
    try:
        seen = await in_own_context(_tenant())
    finally:
        _TENANT.reset(token)

    assert seen == "none"


async def test_what_the_work_sets_does_not_reach_the_caller() -> None:
    async def settle() -> None:
        _ = _TENANT.set("work")

    await in_own_context(settle())

    assert _TENANT.get() == "none"


async def test_it_returns_what_the_work_returns() -> None:
    async def answer() -> int:
        return 42

    assert await in_own_context(answer()) == 42


async def test_what_the_work_raises_reaches_the_caller() -> None:
    async def fail() -> None:
        raise LookupError("missing")

    with pytest.raises(LookupError, match="missing"):
        await in_own_context(fail())


async def test_cancelling_the_caller_cancels_the_work() -> None:
    started = asyncio.Event()
    cancelled: list[bool] = []

    async def wait() -> None:
        started.set()
        try:
            _ = await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled.append(True)
            raise

    caller = asyncio.ensure_future(in_own_context(wait()))
    _ = await asyncio.wait_for(started.wait(), timeout=_TIMEOUT)
    _ = caller.cancel()
    with pytest.raises(asyncio.CancelledError):
        await caller

    assert cancelled == [True]


async def _in_loop() -> bool:
    return started_in_receive_loop()


async def test_a_receive_loop_knows_it_is_one() -> None:
    assert not started_in_receive_loop()
    assert await in_receive_loop(_in_loop())


async def test_a_context_of_its_own_is_not_a_receive_loop_s() -> None:
    async def nested() -> bool:
        return await in_own_context(_in_loop())

    assert not await in_own_context(_in_loop())
    assert not await in_receive_loop(nested())


async def test_the_receive_loop_does_not_see_what_the_caller_s_context_carries() -> None:
    token = _TENANT.set("caller")
    try:
        seen = await in_receive_loop(_tenant())
    finally:
        _TENANT.reset(token)

    assert seen == "none"


async def test_a_task_the_loop_starts_is_in_the_loop_until_it_leaves() -> None:
    async def message() -> tuple[bool, bool]:
        before = started_in_receive_loop()
        leave_receive_loop()
        return before, started_in_receive_loop()

    async def loop() -> tuple[tuple[bool, bool], bool]:
        left = await asyncio.create_task(message())
        return left, started_in_receive_loop()

    assert await in_receive_loop(loop()) == ((True, False), True)
