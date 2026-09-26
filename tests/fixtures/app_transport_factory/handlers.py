"""Handler wired to the container in the counting-transport fixture app."""

from __future__ import annotations

from typing import TYPE_CHECKING, final

from xtr_dependency_injection import as_service

from xtr_messenger import as_message_handler

from .messages import CountJob

if TYPE_CHECKING:
    from uuid import UUID


@final
@as_service()
class CountLedger:
    """A container-built dependency recording every handled job."""

    def __init__(self) -> None:
        self.done: list[UUID] = []


@as_message_handler(CountJob)
@final
class HandleCountJob:
    """Class handler; the container builds it once with its ledger."""

    def __init__(self, ledger: CountLedger) -> None:
        self._ledger = ledger

    async def __call__(self, message: CountJob) -> None:
        self._ledger.done.append(message.job_id)
