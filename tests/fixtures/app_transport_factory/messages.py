"""The message the counting-transport fixture app dispatches."""

from __future__ import annotations

from dataclasses import dataclass
from uuid import UUID

from xtr_messenger import as_message


@as_message(name="tests.fixtures.app_transport_factory.count_job.v1")
@dataclass(frozen=True, slots=True)
class CountJob:
    job_id: UUID
