from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Annotated, ClassVar
from uuid import UUID, uuid4

import pytest
from pydantic import BaseModel, ConfigDict, Field, field_validator

from message_bus import (
    DataclassCodec,
    EncodedEnvelope,
    Envelope,
    JsonSerializer,
    MessageDecodingFailedError,
    as_message,
    default_codecs,
)
from message_bus.transport.serialization import TYPE_HEADER
from message_bus.transport.serialization.codec.pydantic_codec import PydanticCodec


@as_message(name="billing.issue_invoice.v1")
class IssueInvoice(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True, extra="forbid")

    invoice_id: UUID
    amount: Annotated[Decimal, Field(gt=0)]
    currency: str

    @field_validator("currency")
    @classmethod
    def _iso_4217(cls, value: str) -> str:
        if len(value) != 3 or not value.isupper():
            message = "currency must be a three-letter uppercase ISO 4217 code"
            raise ValueError(message)
        return value


@as_message(name="billing.archive.v1")
@dataclass(frozen=True, slots=True)
class ArchiveInvoice:
    invoice_id: UUID


def serializer() -> JsonSerializer:
    return JsonSerializer(codecs=[PydanticCodec(), DataclassCodec()])


def an_invoice() -> IssueInvoice:
    return IssueInvoice(invoice_id=uuid4(), amount=Decimal("10.00"), currency="EUR")


def test_a_pydantic_message_survives_a_round_trip() -> None:
    message = an_invoice()

    decoded = serializer().decode(serializer().encode(Envelope(message)))

    assert decoded.message == message


def test_a_pydantic_message_keeps_its_field_types() -> None:
    message = an_invoice()

    decoded = serializer().decode(serializer().encode(Envelope(message)))

    assert isinstance(decoded.message, IssueInvoice)
    assert isinstance(decoded.message.amount, Decimal)
    assert isinstance(decoded.message.invoice_id, UUID)


def test_a_constraint_violation_is_caught_on_decode() -> None:
    body = f'{{"invoice_id": "{uuid4()}", "amount": "-5", "currency": "EUR"}}'
    encoded = EncodedEnvelope(body=body, headers={TYPE_HEADER: "billing.issue_invoice.v1"})

    with pytest.raises(MessageDecodingFailedError, match="amount"):
        serializer().decode(encoded)


def test_a_custom_validator_runs_on_decode() -> None:
    body = f'{{"invoice_id": "{uuid4()}", "amount": "5", "currency": "eur"}}'
    encoded = EncodedEnvelope(body=body, headers={TYPE_HEADER: "billing.issue_invoice.v1"})

    with pytest.raises(MessageDecodingFailedError, match="ISO 4217"):
        serializer().decode(encoded)


def test_an_unexpected_field_is_caught_on_decode() -> None:
    body = f'{{"invoice_id": "{uuid4()}", "amount": "5", "currency": "EUR", "extra": 1}}'
    encoded = EncodedEnvelope(body=body, headers={TYPE_HEADER: "billing.issue_invoice.v1"})

    with pytest.raises(MessageDecodingFailedError, match="extra"):
        serializer().decode(encoded)


def test_pydantic_failures_surface_as_the_library_error_not_pydantics() -> None:
    body = f'{{"invoice_id": "{uuid4()}", "amount": "-5", "currency": "EUR"}}'
    encoded = EncodedEnvelope(body=body, headers={TYPE_HEADER: "billing.issue_invoice.v1"})

    with pytest.raises(MessageDecodingFailedError) as excinfo:
        serializer().decode(encoded)

    assert excinfo.value.message_name == "IssueInvoice"


def test_dataclass_and_pydantic_messages_share_one_serializer() -> None:
    shared = serializer()
    pydantic_message = an_invoice()
    dataclass_message = ArchiveInvoice(invoice_id=uuid4())

    assert shared.decode(shared.encode(Envelope(pydantic_message))).message == pydantic_message
    assert shared.decode(shared.encode(Envelope(dataclass_message))).message == dataclass_message


def test_a_message_no_codec_recognises_names_the_fix() -> None:
    with pytest.raises(MessageDecodingFailedError, match="JsonSerializer"):
        serializer().encode(Envelope("just a string"))


def test_pydantic_messages_work_without_configuring_a_serializer() -> None:
    default = JsonSerializer()
    message = an_invoice()

    assert default.decode(default.encode(Envelope(message))).message == message


def test_the_default_codecs_prefer_pydantic_when_it_is_installed() -> None:
    assert [type(codec).__name__ for codec in default_codecs()] == [
        "PydanticCodec",
        "DataclassCodec",
    ]


def test_explicit_codecs_override_the_default() -> None:
    dataclass_only = JsonSerializer(codecs=[DataclassCodec()])

    with pytest.raises(MessageDecodingFailedError, match="no codec handles 'IssueInvoice'"):
        dataclass_only.encode(Envelope(an_invoice()))
