"""Stand-ins for the library's collaborators, for testing one unit at a time."""

from __future__ import annotations

from typing import TYPE_CHECKING, final

from typing_extensions import override

from message_bus import Envelope, MessageBusInterface, ReceiverInterface, SenderInterface

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterable

    from message_bus import StampInterface


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
