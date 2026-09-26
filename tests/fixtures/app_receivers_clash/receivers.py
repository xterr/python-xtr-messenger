"""Two receivers claiming the same alias."""

from __future__ import annotations

from typing import TYPE_CHECKING

from typing_extensions import override
from xtr_dependency_injection import as_service, autoconfigure

from xtr_messenger import Envelope, ReceiverInterface
from xtr_messenger.bundle import RECEIVER_TAG

if TYPE_CHECKING:
    from collections.abc import AsyncIterator


class _Empty(ReceiverInterface):
    """Yields nothing."""

    @override
    async def get(self) -> AsyncIterator[Envelope]:
        for envelope in ():
            yield envelope

    @override
    async def ack(self, envelope: Envelope) -> None:
        del envelope

    @override
    async def reject(self, envelope: Envelope) -> None:
        del envelope


@autoconfigure(tags=[(RECEIVER_TAG, {"alias": "same"})])
@as_service
class FirstReceiver(_Empty):
    """Claims ``same``."""


@autoconfigure(tags=[(RECEIVER_TAG, {"alias": "same"})])
@as_service
class SecondReceiver(_Empty):
    """Claims ``same`` too."""
