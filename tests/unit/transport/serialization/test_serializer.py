from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, cast
from uuid import uuid4
from zoneinfo import ZoneInfo

import pytest

from tests.support.messages import (
    IngestDocument,
    UndeclaredMessage,
    assert_dispatch_invoice_is_exact,
    dispatch_invoice,
    ingest_document,
)
from tests.support.serialization import json_serializer
from xtr_messenger import (
    EncodedEnvelope,
    Envelope,
    JsonSerializer,
    MessageDecodingFailedError,
    MessageEncodingFailedError,
    NonSendableStampInterface,
    ReceivedStamp,
    RedeliveryStamp,
    StampInterface,
    TransportMessageIdStamp,
    as_message,
    as_stamp,
)
from xtr_messenger.stamp import DEFAULT_STAMP_TYPES, DelayStamp
from xtr_messenger.transport.serialization import TYPE_HEADER

if TYPE_CHECKING:
    from collections.abc import Callable

ID_STAMP_HEADER = "X-Message-Stamp-TransportMessageIdStamp"
# Stands for a value the wire carried that no error may repeat; it is no credential.
SECRET = "s3cr3t-t0ken"  # noqa: S105 — a marker searched for in error messages


@as_message(name="test.unit.serializer.empty.v1")
@dataclass(frozen=True, slots=True)
class EmptyMessage:
    pass


@as_message(name="test.unit.serializer.naive.v1")
@dataclass(frozen=True, slots=True)
class NaiveMessage:
    at: datetime


def serializer() -> JsonSerializer:
    return json_serializer(stamp_types=[RedeliveryStamp, TransportMessageIdStamp])


def assert_quotes_nothing(error: BaseException, secret: str = SECRET) -> None:
    """Neither the error nor anything it was raised from repeats ``secret``."""
    seen: BaseException | None = error
    while seen is not None:
        assert secret not in str(seen)
        seen = seen.__cause__ or seen.__context__


def test_building_without_a_serializer_fails() -> None:
    build = cast("Callable[[], object]", JsonSerializer)

    with pytest.raises(TypeError, match="serializer"):
        _ = build()


def test_encoding_carries_the_message_name_in_the_type_header() -> None:
    encoded = serializer().encode(Envelope(ingest_document()))

    assert encoded.headers[TYPE_HEADER] == "test.ingest.v1"


def test_a_message_survives_a_round_trip_unchanged() -> None:
    message = IngestDocument(document_id=uuid4(), tenant_id=uuid4(), reversible=True)

    decoded = serializer().decode(serializer().encode(Envelope(message)))

    assert decoded.message == message


def test_a_message_comes_back_exactly_with_every_nested_type() -> None:
    sent = dispatch_invoice()

    decoded = serializer().decode(serializer().encode(Envelope(sent)))

    assert_dispatch_invoice_is_exact(decoded.message, sent)


def test_the_body_writes_the_hierarchy_tag_and_the_zone() -> None:
    body = serializer().encode(Envelope(dispatch_invoice())).body

    assert '"$type":"test.attachment.invoice"' in body
    assert "[Europe/Bucharest]" in body


def test_a_value_the_body_cannot_carry_exactly_is_refused_when_encoding() -> None:
    naive = NaiveMessage(datetime(2026, 1, 1, 9, 30))  # noqa: DTZ001 — the value being refused

    with pytest.raises(MessageEncodingFailedError) as raised:
        _ = serializer().encode(Envelope(naive))

    assert raised.value.message_name == "test.unit.serializer.naive.v1"


def test_registered_stamps_survive_a_round_trip() -> None:
    envelope = Envelope(ingest_document()).with_stamps(TransportMessageIdStamp("abc-123"))

    decoded = serializer().decode(serializer().encode(envelope))

    assert decoded.last(TransportMessageIdStamp) == TransportMessageIdStamp("abc-123")


def test_non_sendable_stamps_never_reach_the_wire() -> None:
    envelope = Envelope(ingest_document()).with_stamps(ReceivedStamp("async"))

    encoded = serializer().encode(envelope)

    assert not any("ReceivedStamp" in header for header in encoded.headers)


def test_a_producers_stamps_survive_a_default_round_trip() -> None:
    wire = json_serializer()
    sent = Envelope(ingest_document()).with_stamps(
        TransportMessageIdStamp("abc"),
        DelayStamp(5000),
    )

    received = wire.decode(wire.encode(sent))

    assert sorted(type(stamp).__name__ for stamp in received.stamps) == [
        "DelayStamp",
        "TransportMessageIdStamp",
    ]


def test_restoring_no_stamps_is_possible_but_must_be_asked_for() -> None:
    wire = json_serializer(stamp_types=())
    sent = Envelope(ingest_document()).with_stamps(DelayStamp(1))

    assert wire.decode(wire.encode(sent)).stamps == ()


def test_no_default_stamp_type_ever_leaves_the_process() -> None:
    """A non-sendable stamp is never written, so restoring it is meaningless."""
    local = [t for t in DEFAULT_STAMP_TYPES if issubclass(t, NonSendableStampInterface)]

    assert local == []


def test_an_unrecognised_stamp_header_is_dropped_not_resolved() -> None:
    """Decoding is an allow-list: a stamp naming a class not in stamp_types is
    dropped rather than imported."""
    encoded = EncodedEnvelope(
        body="{}",
        headers={
            TYPE_HEADER: "test.unit.serializer.empty.v1",
            "X-Message-Stamp-MysteryStamp": '[{"x": 1}]',
        },
    )

    assert serializer().decode(encoded).stamps == ()


def test_decoding_without_a_type_header_fails_loudly() -> None:
    with pytest.raises(MessageDecodingFailedError, match="missing 'type' header"):
        _ = serializer().decode(EncodedEnvelope(body="{}", headers={}))


def test_decoding_an_unknown_message_name_fails_without_repeating_it() -> None:
    encoded = EncodedEnvelope(body="{}", headers={TYPE_HEADER: f"{SECRET}.v1"})

    with pytest.raises(MessageDecodingFailedError, match="names no declared message") as raised:
        _ = serializer().decode(encoded)

    assert raised.value.message_name is None
    assert_quotes_nothing(raised.value)


def test_decoding_an_undeclared_message_fails_loudly() -> None:
    encoded = serializer().encode(Envelope(UndeclaredMessage("x")))

    with pytest.raises(MessageDecodingFailedError, match="names no declared message"):
        _ = serializer().decode(encoded)


def test_decoding_a_body_that_is_not_json_fails_without_repeating_it() -> None:
    encoded = EncodedEnvelope(body="{" + SECRET, headers={TYPE_HEADER: "test.ingest.v1"})

    with pytest.raises(MessageDecodingFailedError) as raised:
        _ = serializer().decode(encoded)

    assert raised.value.message_name == "test.ingest.v1"
    assert_quotes_nothing(raised.value)


def test_decoding_a_forged_body_fails_without_repeating_it() -> None:
    forged = f'{{"document_id": "{SECRET}", "tenant_id": 7}}'
    encoded = EncodedEnvelope(body=forged, headers={TYPE_HEADER: "test.ingest.v1"})

    with pytest.raises(MessageDecodingFailedError) as raised:
        _ = serializer().decode(encoded)

    assert raised.value.message_name == "test.ingest.v1"
    assert_quotes_nothing(raised.value)


def test_decoding_a_forged_hierarchy_tag_fails_without_repeating_it() -> None:
    body = serializer().encode(Envelope(dispatch_invoice())).body
    forged = body.replace("test.attachment.invoice", SECRET)
    encoded = EncodedEnvelope(body=forged, headers={TYPE_HEADER: "test.dispatch_invoice.v1"})

    with pytest.raises(MessageDecodingFailedError) as raised:
        _ = serializer().decode(encoded)

    assert_quotes_nothing(raised.value)


def test_decoding_a_stamp_header_that_is_not_a_list_fails_without_repeating_it() -> None:
    encoded = EncodedEnvelope(
        body="{}",
        headers={
            TYPE_HEADER: "test.unit.serializer.empty.v1",
            ID_STAMP_HEADER: f'{{"message_id": "{SECRET}"}}',
        },
    )

    with pytest.raises(MessageDecodingFailedError) as raised:
        _ = serializer().decode(encoded)

    assert_quotes_nothing(raised.value)


def test_decoding_a_malformed_stamp_fails_loudly() -> None:
    encoded = EncodedEnvelope(
        body="{}",
        headers={
            TYPE_HEADER: "test.unit.serializer.empty.v1",
            ID_STAMP_HEADER: '[{"wrong": "x"}]',
        },
    )

    with pytest.raises(MessageDecodingFailedError, match="message_id"):
        _ = serializer().decode(encoded)


def test_the_wire_header_names_are_pinned() -> None:
    """These strings are the wire contract; a round-trip test would stay green
    even if they changed, so only literals catch a rename."""
    envelope = Envelope(ingest_document()).with_stamps(TransportMessageIdStamp("abc"))

    headers = serializer().encode(envelope).headers

    assert "type" in headers
    assert "X-Message-Stamp-TransportMessageIdStamp" in headers


@as_stamp
@dataclass(frozen=True, slots=True)
class DeclaredTenantStamp(StampInterface):
    """A stamp an application declares."""

    tenant_id: str


@as_stamp
@dataclass(frozen=True, slots=True)
class DeclaredDeadlineStamp(StampInterface):
    """A stamp carrying a value a stamp header must keep exactly."""

    at: datetime


def test_a_declared_stamp_survives_a_default_round_trip() -> None:
    wire = json_serializer()
    envelope = Envelope(ingest_document()).with_stamps(DeclaredTenantStamp("acme"))

    decoded = wire.decode(wire.encode(envelope))

    assert decoded.last(DeclaredTenantStamp) == DeclaredTenantStamp("acme")


def test_a_stamp_value_the_header_cannot_carry_exactly_is_refused_when_encoding() -> None:
    repeated = datetime(2026, 10, 25, 3, 30, tzinfo=ZoneInfo("Europe/Bucharest"), fold=1)
    envelope = Envelope(ingest_document()).with_stamps(DeclaredDeadlineStamp(repeated))

    with pytest.raises(MessageEncodingFailedError):
        _ = json_serializer().encode(envelope)


def test_a_stamp_declared_after_the_serializer_was_built_still_decodes() -> None:
    wire = json_serializer()

    @as_stamp
    @dataclass(frozen=True, slots=True)
    class LateDeclaredStamp(StampInterface):
        value: int

    decoded = wire.decode(
        wire.encode(Envelope(ingest_document()).with_stamps(LateDeclaredStamp(1)))
    )

    assert decoded.last(LateDeclaredStamp) == LateDeclaredStamp(1)


def test_an_explicit_stamp_list_ignores_declared_stamps() -> None:
    wire = json_serializer(stamp_types=[RedeliveryStamp])
    envelope = Envelope(ingest_document()).with_stamps(DeclaredTenantStamp("acme"))

    decoded = wire.decode(wire.encode(envelope))

    assert decoded.last(DeclaredTenantStamp) is None


def test_several_stamps_of_one_type_keep_their_order_through_a_round_trip() -> None:
    stamps = [TransportMessageIdStamp(identifier) for identifier in ("first", "second", "third")]
    envelope = Envelope(ingest_document()).with_stamps(*stamps)

    decoded = serializer().decode(serializer().encode(envelope))

    assert list(decoded.all(TransportMessageIdStamp)) == stamps
