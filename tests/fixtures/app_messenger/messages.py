"""Messages the fixture app dispatches and handles."""

from __future__ import annotations

from dataclasses import dataclass
from uuid import UUID

from xtr_messenger import as_message


@as_message(name="tests.fixtures.app_messenger.do_work.v1")
@dataclass(frozen=True, slots=True)
class DoWork:
    job_id: UUID


@as_message(name="tests.fixtures.app_messenger.audit_event.v1")
@dataclass(frozen=True, slots=True)
class AuditEvent:
    subject: str
