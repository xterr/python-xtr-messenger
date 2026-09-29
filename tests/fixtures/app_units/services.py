"""A scoped session, the journal recording what saw which, and middleware taking arguments."""

from __future__ import annotations

from collections.abc import (
    AsyncIterator,  # noqa: TC003 — the container reads the factory's return annotation
)
from itertools import count
from typing import final

from typing_extensions import override
from xtr_dependency_injection import as_service

from xtr_messenger import Envelope, MiddlewareInterface, StackInterface
from xtr_messenger.decorator import as_middleware
from xtr_messenger.middleware.middleware_registry import MiddlewareRegistry

_SESSIONS = count(1)


@final
class Session:
    """What a unit of work scopes: one per message, closed once the message is done with."""

    def __init__(self) -> None:
        self.number = next(_SESSIONS)
        self.closed = False


@as_service(lifetime="scoped")
async def open_session() -> AsyncIterator[Session]:
    session = Session()
    try:
        yield session
    finally:
        session.closed = True


@final
@as_service
class Journal:
    """Every session a handler saw, and every middleware that ran, in order."""

    def __init__(self) -> None:
        self.sessions: list[tuple[str, Session]] = []
        self.middleware: list[tuple[str, int]] = []


_DECLARED = MiddlewareRegistry()
"""Where the fixture declares its middleware: a registry of its own, not the process-wide one."""


@final
@as_middleware("labelled", registry=_DECLARED)
class LabelledMiddleware(MiddlewareInterface):
    """Records its label as a message passes; the label and count are what a chain may give it."""

    def __init__(self, journal: Journal, label: str = "plain", times: int = 1) -> None:
        self._journal = journal
        self._label = label
        self._times = times

    @override
    async def handle(self, envelope: Envelope, stack: StackInterface, /) -> Envelope:
        self._journal.middleware.append((self._label, self._times))
        return await stack.next().handle(envelope, stack)
