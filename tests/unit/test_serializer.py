from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from enum import Enum, IntEnum
from uuid import UUID, uuid4

import pytest

from message_bus import (
    DataclassCodec,
    EncodedEnvelope,
    Envelope,
    JsonSerializer,
    MessageDecodingFailedError,
    ReceivedStamp,
    RedeliveryStamp,
    TransportMessageIdStamp,
    as_message,
)
from message_bus.transport.serialization import TYPE_HEADER
from tests.conftest import IngestDocument


class Priority(IntEnum):
    LOW = 1
    HIGH = 2


class Channel(Enum):
    EMAIL = "email"
    SMS = "sms"


@as_message(name="test.rich.v1")
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


def serializer() -> JsonSerializer:
    return JsonSerializer(stamp_types=[RedeliveryStamp, TransportMessageIdStamp])


def test_encoding_carries_the_message_name_in_the_type_header() -> None:
    encoded = serializer().encode(Envelope(IngestDocument(uuid4(), uuid4())))

    assert encoded.headers[TYPE_HEADER] == "test.ingest.v1"


def test_a_message_survives_a_round_trip_unchanged() -> None:
    message = IngestDocument(uuid4(), uuid4(), reversible=True, patient_id=uuid4())

    decoded = serializer().decode(serializer().encode(Envelope(message)))

    assert decoded.message == message


def test_every_supported_field_type_survives_a_round_trip() -> None:
    message = RichMessage(
        identifier=uuid4(),
        created_at=datetime(2026, 3, 4, 5, 6, 7, tzinfo=UTC),
        due_on=date(2026, 12, 31),
        amount=Decimal("19.99"),
        priority=Priority.HIGH,
        channel=Channel.SMS,
        tags=["a", "b"],
        labels={"k": "v"},
    )

    decoded = serializer().decode(serializer().encode(Envelope(message)))

    assert decoded.message == message


def test_registered_stamps_survive_a_round_trip() -> None:
    envelope = Envelope(IngestDocument(uuid4(), uuid4())).with_stamps(
        TransportMessageIdStamp("abc-123"),
    )

    decoded = serializer().decode(serializer().encode(envelope))

    assert decoded.last(TransportMessageIdStamp) == TransportMessageIdStamp("abc-123")


def test_non_sendable_stamps_never_reach_the_wire() -> None:
    envelope = Envelope(IngestDocument(uuid4(), uuid4())).with_stamps(ReceivedStamp("async"))

    encoded = serializer().encode(envelope)

    assert not any("ReceivedStamp" in header for header in encoded.headers)


def test_decoding_without_a_type_header_fails_loudly() -> None:
    with pytest.raises(MessageDecodingFailedError, match="missing 'type' header"):
        serializer().decode(EncodedEnvelope(body="{}", headers={}))


def test_decoding_an_unknown_message_name_fails_loudly() -> None:
    encoded = EncodedEnvelope(body="{}", headers={TYPE_HEADER: "nope.v1"})

    with pytest.raises(MessageDecodingFailedError, match=re.escape("nope.v1")):
        serializer().decode(encoded)


def test_decoding_a_malformed_body_fails_loudly() -> None:
    encoded = EncodedEnvelope(body="{not json", headers={TYPE_HEADER: "test.ingest.v1"})

    with pytest.raises(MessageDecodingFailedError, match="not valid JSON"):
        serializer().decode(encoded)


def test_a_missing_field_fails_loudly_instead_of_arriving_incomplete() -> None:
    encoded = EncodedEnvelope(
        body='{"document_id": "%s"}' % uuid4(),  # noqa: UP031
        headers={TYPE_HEADER: "test.ingest.v1"},
    )

    # msgspec names the field that is missing, which the old message did not.
    with pytest.raises(MessageDecodingFailedError, match="tenant_id"):
        serializer().decode(encoded)


def test_a_field_the_message_does_not_declare_is_ignored() -> None:
    """A newer producer must not break an older consumer.

    Rejecting unknown fields would make adding one to a message a
    coordinated deploy of every consumer first.
    """
    body = f'{{"document_id": "{uuid4()}", "tenant_id": "{uuid4()}", "added_later": 1}}'
    encoded = EncodedEnvelope(body=body, headers={TYPE_HEADER: "test.ingest.v1"})

    decoded = serializer().decode(encoded)

    assert isinstance(decoded.message, IngestDocument)


def test_unknown_fields_can_be_forbidden_for_a_bus_that_deploys_together() -> None:
    body = f'{{"document_id": "{uuid4()}", "tenant_id": "{uuid4()}", "typo": 1}}'
    encoded = EncodedEnvelope(body=body, headers={TYPE_HEADER: "test.ingest.v1"})
    strict = JsonSerializer(codecs=[DataclassCodec(forbid_unknown_fields=True)])

    with pytest.raises(MessageDecodingFailedError, match="unknown field `typo`"):
        strict.decode(encoded)


def test_a_field_of_the_wrong_shape_fails_loudly_instead_of_being_coerced() -> None:
    body = f'{{"document_id": 7, "tenant_id": "{uuid4()}"}}'
    encoded = EncodedEnvelope(body=body, headers={TYPE_HEADER: "test.ingest.v1"})

    # Not coerced, and the error points at the offending field by JSON path.
    with pytest.raises(MessageDecodingFailedError, match=r"uuid.*document_id"):
        serializer().decode(encoded)


def test_encoding_a_message_no_codec_recognises_fails_loudly() -> None:
    with pytest.raises(MessageDecodingFailedError, match="no codec handles 'str'"):
        serializer().encode(Envelope("just a string"))


def test_the_wire_header_names_are_pinned() -> None:
    """These strings are the wire contract: a rename must not reach them.

    Encode and decode share the constants, so a round-trip test stays green
    even if the names change underneath. Only literals catch that.
    """
    envelope = Envelope(IngestDocument(uuid4(), uuid4())).with_stamps(
        TransportMessageIdStamp("abc"),
    )

    headers = serializer().encode(envelope).headers

    assert "type" in headers
    assert "X-Message-Stamp-TransportMessageIdStamp" in headers
