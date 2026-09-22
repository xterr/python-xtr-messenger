"""A transport that handles the message in the calling process."""

from __future__ import annotations

from typing import TYPE_CHECKING, final

from typing_extensions import override

from message_bus.stamp import HandledStamp
from message_bus.transport.transport_interface import TransportInterface

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from message_bus.envelope import Envelope
    from message_bus.handler import HandlersLocatorInterface

__all__ = ["SyncTransport"]


@final
class SyncTransport(TransportInterface):
    """Runs the registered handlers immediately, in the caller's task.

    Useful for local development and for messages that do not need a worker:
    the dispatch site keeps the same API, so moving a message onto a real
    queue later is a routing-table change and nothing else.

    Handler exceptions propagate to the dispatcher — there is no queue to
    retry from, so failing loudly is the honest behaviour.

    The receive half is empty, which describes this transport accurately
    rather than leaving a gap: the work is finished by the time :meth:`send`
    returns, so there is never anything left for a worker to collect. A
    worker pointed here starts, finds nothing outstanding, and stops — which
    beats raising, because it lets one worker entrypoint serve a list of
    transports that happens to include ``sync://``.
    """

    __slots__ = ("_registry",)

    def __init__(self, registry: HandlersLocatorInterface) -> None:
        """Resolve handlers from ``registry`` at send time."""
        self._registry = registry

    @override
    async def send(self, envelope: Envelope) -> Envelope:
        """Invoke every handler bound to the message, stamping each one."""
        for descriptor in self._registry.handlers_for(type(envelope.message)):
            await descriptor.invoke(envelope)
            envelope = envelope.with_stamps(HandledStamp(descriptor.name))
        return envelope

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
