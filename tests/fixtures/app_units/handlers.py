"""Handlers recording the session each was given: two of a message, a nested one, a follow-up."""

from __future__ import annotations

# The container reads the handlers' annotations at runtime.
from xtr_dependency_injection import Injected  # noqa: TC002

from xtr_messenger import DispatchAfterCurrentBusStamp, MessageBusInterface, as_message_handler

from .messages import FollowUp, Nested, Queued, Record
from .services import Journal, Session  # noqa: TC001


@as_message_handler(Record)
async def first(message: Record, session: Injected[Session], journal: Injected[Journal]) -> None:
    journal.sessions.append((f"first:{message.value}", session))


@as_message_handler(Record)
async def second(
    message: Record,
    session: Injected[Session],
    journal: Injected[Journal],
    bus: Injected[MessageBusInterface],
) -> None:
    journal.sessions.append((f"second:{message.value}", session))
    _ = await bus.dispatch(Nested(message.value))


@as_message_handler(Nested)
async def nested(message: Nested, session: Injected[Session], journal: Injected[Journal]) -> None:
    journal.sessions.append((f"nested:{message.value}", session))


@as_message_handler(Queued)
async def queued(
    message: Queued,
    session: Injected[Session],
    journal: Injected[Journal],
    bus: Injected[MessageBusInterface],
) -> None:
    journal.sessions.append((f"queued:{message.value}", session))
    _ = await bus.dispatch(FollowUp(message.value), DispatchAfterCurrentBusStamp())


@as_message_handler(FollowUp)
async def follow_up(
    message: FollowUp, session: Injected[Session], journal: Injected[Journal]
) -> None:
    journal.sessions.append((f"follow_up:{message.value}", session))
