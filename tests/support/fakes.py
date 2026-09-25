"""Stand-ins for the library's collaborators, for testing one unit at a time."""

from __future__ import annotations

from typing import TYPE_CHECKING, final

from typing_extensions import override

from xtr_messenger import (
    Envelope,
    MessageBusInterface,
    MiddlewareInterface,
    ReceiverInterface,
    SenderInterface,
    StackInterface,
)

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterable

    from xtr_messenger import StampInterface


@final
class RecordingMiddleware(MiddlewareInterface):
    """Notes ``label`` in ``calls``, then continues the chain."""

    def __init__(self, calls: list[str] | None = None, label: str = "middleware") -> None:
        self.calls = calls if calls is not None else []
        self._label = label

    @override
    async def handle(self, envelope: Envelope, stack: StackInterface, /) -> Envelope:
        self.calls.append(self._label)
        return await stack.next().handle(envelope, stack)


@final
class TerminalMiddleware(MiddlewareInterface):
    """Ends the chain, keeping each envelope handed to it — proof the chain got this far."""

    def __init__(self) -> None:
        self.seen: list[Envelope] = []

    @override
    async def handle(self, envelope: Envelope, stack: StackInterface, /) -> Envelope:
        del stack
        self.seen.append(envelope)
        return envelope


@final
class OneStep(StackInterface):
    """A stack that hands back a single middleware — the one under test's successor."""

    def __init__(self, terminal: MiddlewareInterface | None = None) -> None:
        self._terminal = terminal if terminal is not None else TerminalMiddleware()

    @override
    def next(self) -> MiddlewareInterface:
        return self._terminal


@final
class RecordingBus(MessageBusInterface):
    """A bus that records what it is given, optionally failing every dispatch."""

    def __init__(self, failure: Exception | None = None) -> None:
        self.dispatched: list[Envelope] = []
        self._failure = failure

    @override
    async def dispatch(self, message: object, *stamps: StampInterface) -> Envelope:
        envelope = Envelope.wrap(message, stamps)
        self.dispatched.append(envelope)
        if self._failure is not None:
            raise self._failure
        return envelope


@final
class RecordingSender(SenderInterface):
    """A sender that records what it is given and hands it back as it was."""

    def __init__(self) -> None:
        self.sent: list[Envelope] = []

    @override
    async def send(self, envelope: Envelope) -> Envelope:
        self.sent.append(envelope)
        return envelope


@final
class StubReceiver(ReceiverInterface):
    """A receiver yielding a fixed backlog and recording how each was settled."""

    def __init__(self, backlog: Iterable[Envelope] = ()) -> None:
        self._backlog = tuple(backlog)
        self.acked: list[Envelope] = []
        self.rejected: list[Envelope] = []

    @override
    async def get(self) -> AsyncIterator[Envelope]:
        for envelope in self._backlog:
            yield envelope

    @override
    async def ack(self, envelope: Envelope) -> None:
        self.acked.append(envelope)

    @override
    async def reject(self, envelope: Envelope) -> None:
        self.rejected.append(envelope)
