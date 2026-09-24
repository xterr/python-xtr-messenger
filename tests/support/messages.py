"""Messages shared across the suite.

Wire names are process-wide, so every message a test declares needs a name no
other test module uses. These are the common ones; a test needing a message
of its own declares it locally under a name prefixed with its module.
"""

from __future__ import annotations

from dataclasses import dataclass
from uuid import UUID, uuid4

from xtr_messenger import as_message


@as_message(name="test.ingest.v1")
@dataclass(frozen=True, slots=True)
class IngestDocument:
    document_id: UUID
    tenant_id: UUID
    reversible: bool = False
    patient_id: UUID | None = None


@as_message(name="test.analyse.v1")
@dataclass(frozen=True, slots=True)
class AnalyseDocument:
    document_id: UUID


@dataclass(frozen=True, slots=True)
class UndeclaredMessage:
    """Never declared with ``@as_message``: travels under ``module:QualName``."""

    payload: str


def ingest_document() -> IngestDocument:
    return IngestDocument(document_id=uuid4(), tenant_id=uuid4())
