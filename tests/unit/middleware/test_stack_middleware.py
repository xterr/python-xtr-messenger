"""The cursor that walks a middleware chain, and ends it."""

from __future__ import annotations

import pytest

from tests.support.fakes import RecordingMiddleware
from xtr_messenger import Envelope, StackMiddleware

pytestmark = pytest.mark.anyio


def test_it_yields_each_middleware_in_order_then_itself() -> None:
    """Walking past the last middleware returns the cursor itself, so a
    middleware at the tail can delegate unconditionally."""
    first, second = RecordingMiddleware(), RecordingMiddleware()
    stack = StackMiddleware([first, second])

    assert stack.next() is first
    assert stack.next() is second
    assert stack.next() is stack


async def test_the_tail_returns_the_envelope_unchanged() -> None:
    stack = StackMiddleware([])
    envelope = Envelope("payload").with_stamps()

    result = await stack.next().handle(envelope, stack)

    assert result is envelope


async def test_an_empty_chain_is_a_no_op_dispatch() -> None:
    stack = StackMiddleware([])

    assert stack.next() is stack
    assert await stack.next().handle(Envelope("payload"), stack) == Envelope("payload")


def test_the_cursor_is_single_use_and_does_not_rewind() -> None:
    only = RecordingMiddleware()
    stack = StackMiddleware([only])

    assert stack.next() is only
    assert stack.next() is stack
    assert stack.next() is stack
