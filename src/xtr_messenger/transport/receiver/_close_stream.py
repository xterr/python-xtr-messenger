"""Closing what a receiver handed out."""

from __future__ import annotations

import inspect
from typing import TYPE_CHECKING, cast

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Awaitable

    from xtr_messenger.envelope import Envelope

__all__ = ["close_stream"]


async def close_stream(stream: AsyncIterator[Envelope]) -> None:
    """Close ``stream`` if it can be, so a receiver lets go of what it holds now, not later.

    A receiver's generator releasing a channel in ``finally`` would otherwise
    do so whenever the loop collects it — after the worker said it stopped.
    """
    close: object = getattr(stream, "aclose", None)
    if callable(close):
        closing: object = close()
        if inspect.isawaitable(closing):
            _ = await cast("Awaitable[object]", closing)
