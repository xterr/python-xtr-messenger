"""A receiver the application registers: it only produces messages."""

from __future__ import annotations

from typing import TYPE_CHECKING

from typing_extensions import override
from xtr_dependency_injection import as_service, autoconfigure

from xtr_messenger import Envelope, ReceivedStamp, ReceiverInterface
from xtr_messenger.bundle import RECEIVER_TAG

from .messages import Tick

if TYPE_CHECKING:
    from collections.abc import AsyncIterator


@autoconfigure(tags=[(RECEIVER_TAG, {"alias": "ticks"})])
@as_service
class TickReceiver(ReceiverInterface):
    """Produces three ticks, then is exhausted."""

    @override
    async def get(self) -> AsyncIterator[Envelope]:
        for number in range(3):
            yield Envelope(Tick(number), (ReceivedStamp("ticks"),))

    @override
    async def ack(self, envelope: Envelope) -> None:
        del envelope

    @override
    async def reject(self, envelope: Envelope) -> None:
        del envelope
