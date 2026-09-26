"""Handlers wired to the container in the fixture app."""

from __future__ import annotations

from typing import TYPE_CHECKING, Annotated, final

from xtr_dependency_injection import Injected, Target, as_service

if TYPE_CHECKING:
    from uuid import UUID

from xtr_messenger import as_message_handler

from .messages import AuditEvent, DoWork


@final
@as_service()
class Ledger:
    """A container-built dependency the class handler asks for."""

    def __init__(self) -> None:
        self.done: list[UUID] = []
        self.audited: list[str] = []


@as_message_handler(DoWork)
@final
class HandleDoWork:
    """Class handler; the container builds it once with its ledger."""

    def __init__(self, ledger: Ledger) -> None:
        self._ledger = ledger

    async def __call__(self, message: DoWork) -> None:
        self._ledger.done.append(message.job_id)


@as_message_handler(AuditEvent)
async def audit(message: AuditEvent, ledger: Injected[Ledger]) -> None:
    """Function handler; the ledger is filled by the container."""
    ledger.audited.append(message.subject)


@as_service(qualifier="archive")
def archive_ledger() -> Ledger:
    """A second ledger, told apart by its qualifier."""
    return Ledger()


@as_message_handler(DoWork)
async def archive(message: DoWork, ledger: Annotated[Ledger, Target("archive")]) -> None:
    """Function handler; the qualified ledger is filled by the container."""
    ledger.done.append(message.job_id)
