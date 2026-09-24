from __future__ import annotations

from decimal import Decimal
from typing import TYPE_CHECKING, Annotated, ClassVar
from uuid import UUID, uuid4

import pytest
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from xtr_messenger import MessageDecodingFailedError, MessageEncodingFailedError, as_message
from xtr_messenger.transport.serialization.codec.pydantic_codec import PydanticCodec

if TYPE_CHECKING:
    from xtr_messenger.transport.serialization.codec import JsonValue


@as_message(name="test.unit.pydantic_codec.issue_invoice.v1")
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


@as_message(name="test.unit.pydantic_codec.paired.v1")
class Paired(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True)

    low: int
    high: int

    @model_validator(mode="after")
    def _ordered(self) -> Paired:
        if self.low > self.high:
            message = "low must not exceed high"
            raise ValueError(message)
        return self


class NotAModel:
    pass


def an_invoice() -> IssueInvoice:
    return IssueInvoice(invoice_id=uuid4(), amount=Decimal("10.00"), currency="EUR")


def test_it_supports_pydantic_models() -> None:
    assert PydanticCodec().supports(IssueInvoice)


def test_it_does_not_support_a_plain_class() -> None:
    assert not PydanticCodec().supports(NotAModel)


def test_a_model_survives_a_round_trip_keeping_its_field_types() -> None:
    codec = PydanticCodec()
    message = an_invoice()

    decoded = codec.decode(IssueInvoice, codec.encode(message))

    assert decoded == message
    assert isinstance(decoded, IssueInvoice)
    assert isinstance(decoded.amount, Decimal)
    assert isinstance(decoded.invoice_id, UUID)


def test_encoding_something_that_is_not_a_model_fails_loudly() -> None:
    with pytest.raises(MessageEncodingFailedError, match="not a pydantic model"):
        _ = PydanticCodec().encode(NotAModel())


def test_a_constraint_violation_is_caught_on_decode() -> None:
    raw: JsonValue = {"invoice_id": str(uuid4()), "amount": "-5", "currency": "EUR"}

    with pytest.raises(MessageDecodingFailedError, match="amount"):
        _ = PydanticCodec().decode(IssueInvoice, raw)


def test_a_custom_validator_runs_on_decode() -> None:
    raw: JsonValue = {"invoice_id": str(uuid4()), "amount": "5", "currency": "eur"}

    with pytest.raises(MessageDecodingFailedError, match="ISO 4217"):
        _ = PydanticCodec().decode(IssueInvoice, raw)


def test_an_unexpected_field_is_caught_on_decode() -> None:
    raw: JsonValue = {"invoice_id": str(uuid4()), "amount": "5", "currency": "EUR", "extra": 1}

    with pytest.raises(MessageDecodingFailedError, match="extra"):
        _ = PydanticCodec().decode(IssueInvoice, raw)


def test_a_model_level_failure_is_reported_against_the_root() -> None:
    """A cross-field invariant has no single field to blame, so it surfaces at
    the model root rather than naming nothing."""
    raw: JsonValue = {"low": 5, "high": 1}

    with pytest.raises(MessageDecodingFailedError, match="high"):
        _ = PydanticCodec().decode(Paired, raw)


def test_a_failure_names_the_model_not_pydantics_error() -> None:
    """A caller never needs to import pydantic to catch a decode failure."""
    raw: JsonValue = {"invoice_id": str(uuid4()), "amount": "-5", "currency": "EUR"}

    with pytest.raises(MessageDecodingFailedError) as excinfo:
        _ = PydanticCodec().decode(IssueInvoice, raw)

    assert excinfo.value.message_name == "IssueInvoice"


def test_decoding_into_a_non_model_type_fails_loudly() -> None:
    with pytest.raises(MessageDecodingFailedError, match="not a pydantic model"):
        _ = PydanticCodec().decode(NotAModel, {})
