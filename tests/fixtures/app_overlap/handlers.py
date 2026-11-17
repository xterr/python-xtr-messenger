"""A message whose handler waits until every message is in flight, then logs and records."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

# The container reads the handler's annotations at runtime.
from xtr_dependency_injection import Injected  # noqa: TC002
from xtr_logging_contracts import LoggerInterface  # noqa: TC002

from xtr_messenger import as_message, as_message_handler

from .services import Remembering, Session  # noqa: TC001

if TYPE_CHECKING:
    import asyncio

__all__ = ["IN_FLIGHT", "SEEN", "Overlapping", "handle"]

IN_FLIGHT: list[asyncio.Barrier] = []
"""The barrier every handler waits at; a test puts one in, sized to the messages it sends."""

SEEN: list[tuple[str, Session]] = []
"""Each message's value with the session it was handled with."""


@as_message(name="tests.fixtures.app_overlap.overlapping.v1")
@dataclass(frozen=True, slots=True)
class Overlapping:
    value: str


@as_message_handler(Overlapping)
async def handle(
    message: Overlapping,
    session: Injected[Session],
    logger: Injected[LoggerInterface],
    remembering: Injected[Remembering],
) -> None:
    logger.info("handling {value}", {"value": message.value})
    remembering.noted.append(message.value)
    _ = await IN_FLIGHT[0].wait()
    SEEN.append((message.value, session))
    if message.value == "boom":
        logger.error("failed {value}", {"value": message.value})
