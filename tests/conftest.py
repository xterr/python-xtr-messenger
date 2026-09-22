from __future__ import annotations

from dataclasses import dataclass
from uuid import UUID

import pytest

from message_bus import as_message


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


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
class UnroutedMessage:
    payload: str
