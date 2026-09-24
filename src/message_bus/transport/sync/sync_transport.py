"""A transport that handles the message in the calling process."""

from __future__ import annotations

from typing import TYPE_CHECKING, final

from typing_extensions import override

from message_bus.stamp import ReceivedStamp, SentStamp
from message_bus.transport.transport_interface import TransportInterface

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from message_bus.envelope import Envelope

__all__ = ["SyncTransport"]


@final
class SyncTransport(TransportInterface):
    """Has the message handled immediately, by the bus that dispatched it.

    Sending marks the envelope received and hands it straight back, so
    :class:`~message_bus.middleware.SendMessageMiddleware` passes it on to
    the bus's own handling step instead of stopping there. The transport
    never calls a handler itself, so which handlers run is decided in one
    place — the bus — whether a message is handled here or by a worker.

    Useful for local development and for messages that do not need a worker:
    the dispatch site keeps the same API, so moving a message onto a real
    queue later is a routing-table change and nothing else.

    Handler exceptions propagate to the dispatcher — there is no queue to
    retry from, so failing loudly is the honest behaviour.

    The receive half is empty, which describes this transport accurately
    rather than leaving a gap: the work is finished by the time the dispatch
    returns, so there is never anything left for a worker to collect. A
    worker pointed here starts, finds nothing outstanding, and stops — which
    beats raising, because it lets one worker entrypoint serve a list of
    transports that happens to include ``sync://``.
    """

    __slots__ = ()

    @override
    async def send(self, envelope: Envelope) -> Envelope:
        """Mark the envelope received, under the name it was sent to."""
        sent = envelope.last(SentStamp)
        return envelope.with_stamps(ReceivedStamp(sent.sender_alias if sent else "sync"))

    @override
    def get(self) -> AsyncIterator[Envelope]:
        """Yield nothing — a sync transport keeps no backlog to collect."""
        return _nothing_to_collect()

    @override
    async def ack(self, envelope: Envelope) -> None:
        """Do nothing — nothing was ever left outstanding."""
        del envelope

    @override
    async def reject(self, envelope: Envelope) -> None:
        """Do nothing — a failure here already reached the dispatcher."""
        del envelope


async def _nothing_to_collect() -> AsyncIterator[Envelope]:
    """Return an async iterator that completes without yielding."""
    backlog: tuple[Envelope, ...] = ()
    for envelope in backlog:
        yield envelope
