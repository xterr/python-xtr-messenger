from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from enum import Enum, IntEnum
from typing import TYPE_CHECKING
from uuid import UUID, uuid4

import pytest

from tests.support.messages import IngestDocument
from xtr_messenger import (
    DataclassCodec,
    MessageDecodingFailedError,
    MessageEncodingFailedError,
    as_message,
)

if TYPE_CHECKING:
    from xtr_messenger.transport.serialization.codec import JsonValue


class Priority(IntEnum):
    LOW = 1
    HIGH = 2


class Channel(Enum):
    EMAIL = "email"
    SMS = "sms"


@as_message(name="test.unit.dataclass_codec.rich.v1")
@dataclass(frozen=True, slots=True)
class RichMessage:
    identifier: UUID
    created_at: datetime
    due_on: date
    amount: Decimal
    priority: Priority
    channel: Channel
    tags: list[str]
    labels: dict[str, str]
    note: str | None = None


@dataclass(frozen=True, slots=True)
class Inner:
    value: int


@as_message(name="test.unit.dataclass_codec.outer.v1")
@dataclass(frozen=True, slots=True)
class Outer:
    inner: Inner


@as_message(name="test.unit.dataclass_codec.twins.v1")
@dataclass(frozen=True, slots=True)
class Twins:
    left: Inner
    right: Inner


@as_message(name="test.unit.dataclass_codec.has_object.v1")
@dataclass(frozen=True, slots=True)
class HasObject:
    thing: object


def a_rich_message() -> RichMessage:
    return RichMessage(
        identifier=uuid4(),
        created_at=datetime(2026, 3, 4, 5, 6, 7, tzinfo=UTC),
        due_on=date(2026, 12, 31),
        amount=Decimal("19.99"),
        priority=Priority.HIGH,
        channel=Channel.SMS,
        tags=["a", "b"],
        labels={"k": "v"},
    )


def test_it_supports_only_dataclasses() -> None:
    codec = DataclassCodec()

    assert codec.supports(IngestDocument)
    assert not codec.supports(str)


def test_every_supported_field_type_survives_a_round_trip() -> None:
    codec = DataclassCodec()
    message = a_rich_message()

    decoded = codec.decode(RichMessage, codec.encode(message))

    assert decoded == message


def test_a_missing_required_field_fails_loudly() -> None:
    """msgspec names the field that is missing rather than arriving incomplete."""
    raw: JsonValue = {"document_id": str(uuid4())}

    with pytest.raises(MessageDecodingFailedError, match="tenant_id"):
        _ = DataclassCodec().decode(IngestDocument, raw)


def test_a_field_of_the_wrong_type_is_not_coerced() -> None:
    """Type-strict: a value of the wrong JSON type is an error, not a coercion."""
    raw: JsonValue = {"document_id": 7, "tenant_id": str(uuid4())}

    with pytest.raises(MessageDecodingFailedError, match="document_id"):
        _ = DataclassCodec().decode(IngestDocument, raw)


def test_an_unknown_field_is_ignored_by_default() -> None:
    """A newer producer must not break an older consumer, so an unknown field
    is ignored rather than making every schema addition a coordinated deploy."""
    raw: JsonValue = {"document_id": str(uuid4()), "tenant_id": str(uuid4()), "added_later": 1}

    decoded = DataclassCodec().decode(IngestDocument, raw)

    assert isinstance(decoded, IngestDocument)


def test_unknown_fields_can_be_forbidden() -> None:
    raw: JsonValue = {"document_id": str(uuid4()), "tenant_id": str(uuid4()), "typo": 1}
    strict = DataclassCodec(forbid_unknown_fields=True)

    with pytest.raises(MessageDecodingFailedError, match="typo"):
        _ = strict.decode(IngestDocument, raw)


def test_forbidding_unknown_fields_accepts_a_clean_message() -> None:
    raw: JsonValue = {"document_id": str(uuid4()), "tenant_id": str(uuid4())}
    strict = DataclassCodec(forbid_unknown_fields=True)

    decoded = strict.decode(IngestDocument, raw)

    assert isinstance(decoded, IngestDocument)


def test_the_strict_mirror_is_reused_across_decodes() -> None:
    strict = DataclassCodec(forbid_unknown_fields=True)
    raw: JsonValue = {"document_id": str(uuid4()), "tenant_id": str(uuid4())}

    _ = strict.decode(IngestDocument, raw)
    decoded = strict.decode(IngestDocument, raw)

    assert isinstance(decoded, IngestDocument)


def test_unknown_fields_are_forbidden_at_every_level_of_nesting() -> None:
    """It rejects at every level, naming the field and its JSON path."""
    raw: JsonValue = {"inner": {"value": 1, "typo": 2}}
    strict = DataclassCodec(forbid_unknown_fields=True)

    with pytest.raises(MessageDecodingFailedError, match=re.escape("$.inner")):
        _ = strict.decode(Outer, raw)


def test_a_repeated_nested_type_is_mirrored_once() -> None:
    """The mirror of a nested type is reused within one message, not rebuilt."""
    raw: JsonValue = {"left": {"value": 1}, "right": {"value": 2}}
    strict = DataclassCodec(forbid_unknown_fields=True)

    decoded = strict.decode(Twins, raw)

    assert isinstance(decoded, Twins)


def test_encoding_a_field_with_no_json_form_fails_loudly() -> None:
    codec = DataclassCodec()

    with pytest.raises(MessageEncodingFailedError) as excinfo:
        _ = codec.encode(HasObject(thing=object()))

    assert excinfo.value.message_name == "HasObject"
