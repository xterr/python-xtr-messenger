"""Messages shared across the suite.

Wire names are process-wide, so every message a test declares needs a name no
other test module uses. These are the common ones; a test needing a message
of its own declares it locally under a name prefixed with its module.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import Enum
from uuid import UUID, uuid4
from zoneinfo import ZoneInfo

from xtr_serializer import ZonedDateTime, discriminator_map, discriminator_name

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


class Priority(Enum):
    LOW = 1
    HIGH = 2


@discriminator_map()
@dataclass(frozen=True, slots=True)
class Attachment:
    """The base of a hierarchy: a field typed with it carries any named subclass."""

    name: str


@discriminator_name("test.attachment.invoice")
@dataclass(frozen=True, slots=True)
class InvoiceAttachment(Attachment):
    total: Decimal


@dataclass(frozen=True, slots=True)
class Recipient:
    address: str
    priority: Priority


@as_message(name="test.dispatch_invoice.v1")
@dataclass(frozen=True, slots=True)
class DispatchInvoice:
    """Every kind of value a body must carry exactly."""

    recipient: Recipient
    due_at: ZonedDateTime
    amount: Decimal
    priority: Priority
    attachment: Attachment


def dispatch_invoice() -> DispatchInvoice:
    """A message whose due date falls in the hour the clocks repeat."""
    return DispatchInvoice(
        recipient=Recipient("ana@example.com", Priority.HIGH),
        due_at=datetime(2026, 10, 25, 3, 30, tzinfo=ZoneInfo("Europe/Bucharest"), fold=1),
        amount=Decimal("1999.990"),
        priority=Priority.LOW,
        attachment=InvoiceAttachment("invoice.pdf", Decimal("12.50")),
    )


def assert_dispatch_invoice_is_exact(received: object, sent: DispatchInvoice) -> None:
    """Assert ``received`` equals ``sent`` and every nested value kept its type and zone."""
    assert isinstance(received, DispatchInvoice)
    assert received == sent
    assert type(received.recipient) is Recipient
    assert type(received.attachment) is InvoiceAttachment
    assert type(received.amount) is Decimal
    assert str(received.amount) == str(sent.amount)
    assert received.priority is sent.priority
    assert received.recipient.priority is sent.recipient.priority
    assert (received.due_at.tzinfo, received.due_at.fold) == (sent.due_at.tzinfo, 1)
