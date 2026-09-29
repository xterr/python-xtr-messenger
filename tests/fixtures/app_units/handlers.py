"""Two handlers of one message and a nested dispatch, each recording the session it was given."""

from __future__ import annotations

# The container reads the handlers' annotations at runtime.
from xtr_dependency_injection import Injected  # noqa: TC002

from xtr_messenger import MessageBusInterface, as_message_handler

from .messages import Nested, Record
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
