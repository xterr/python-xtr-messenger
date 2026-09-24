from __future__ import annotations

import re
from dataclasses import dataclass
from typing import final
from uuid import uuid4

import pytest
from typing_extensions import override

from message_bus import (
    EncodedEnvelope,
    Envelope,
    JsonSerializer,
    MessageDecodingFailedError,
    MessageEncodingFailedError,
    NonSendableStampInterface,
    ReceivedStamp,
    RedeliveryStamp,
    TransportMessageIdStamp,
    as_message,
)
from message_bus.stamp import DEFAULT_STAMP_TYPES, DelayStamp
from message_bus.transport.serialization import TYPE_HEADER
from message_bus.transport.serialization.codec import JsonValue, MessageCodecInterface
from tests.support.messages import IngestDocument, ingest_document

ID_STAMP_HEADER = "X-Message-Stamp-TransportMessageIdStamp"


@as_message(name="test.unit.serializer.empty.v1")
@dataclass(frozen=True, slots=True)
class EmptyMessage:
    pass


@final
class SpyCodec(MessageCodecInterface):
    """A codec that claims one type and counts its encodes, for order tests."""

    def __init__(self, handled: type) -> None:
        self._handled = handled
        self.encoded = 0

    @override
    def supports(self, message_type: type) -> bool:
        return message_type is self._handled

    @override
    def encode(self, message: object) -> JsonValue:
        self.encoded += 1
        return {}

    @override
    def decode(self, message_type: type, raw: JsonValue) -> object:
        raise NotImplementedError


def serializer() -> JsonSerializer:
    return JsonSerializer(stamp_types=[RedeliveryStamp, TransportMessageIdStamp])


def test_encoding_carries_the_message_name_in_the_type_header() -> None:
    encoded = serializer().encode(Envelope(ingest_document()))

    assert encoded.headers[TYPE_HEADER] == "test.ingest.v1"


def test_a_message_survives_a_round_trip_unchanged() -> None:
    message = IngestDocument(document_id=uuid4(), tenant_id=uuid4(), reversible=True)

    decoded = serializer().decode(serializer().encode(Envelope(message)))

    assert decoded.message == message


def test_registered_stamps_survive_a_round_trip() -> None:
    envelope = Envelope(ingest_document()).with_stamps(TransportMessageIdStamp("abc-123"))

    decoded = serializer().decode(serializer().encode(envelope))

    assert decoded.last(TransportMessageIdStamp) == TransportMessageIdStamp("abc-123")


def test_non_sendable_stamps_never_reach_the_wire() -> None:
    envelope = Envelope(ingest_document()).with_stamps(ReceivedStamp("async"))

    encoded = serializer().encode(envelope)

    assert not any("ReceivedStamp" in header for header in encoded.headers)


def test_a_producers_stamps_survive_a_default_round_trip() -> None:
    """The default serializer once wrote every stamp and restored none, so an
    un-customised deploy dropped everything a producer attached."""
    wire = JsonSerializer()
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
    wire = JsonSerializer(stamp_types=())
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


def test_decoding_an_unknown_message_name_fails_loudly() -> None:
    encoded = EncodedEnvelope(body="{}", headers={TYPE_HEADER: "nope.v1"})

    with pytest.raises(MessageDecodingFailedError, match=re.escape("nope.v1")):
        _ = serializer().decode(encoded)


def test_decoding_a_malformed_body_fails_loudly() -> None:
    encoded = EncodedEnvelope(body="{not json", headers={TYPE_HEADER: "test.ingest.v1"})

    with pytest.raises(MessageDecodingFailedError, match="not valid JSON"):
        _ = serializer().decode(encoded)


def test_decoding_a_stamp_header_that_is_not_a_list_fails_loudly() -> None:
    encoded = EncodedEnvelope(
        body="{}",
        headers={
            TYPE_HEADER: "test.unit.serializer.empty.v1",
            ID_STAMP_HEADER: '{"message_id": "x"}',
        },
    )

    with pytest.raises(MessageDecodingFailedError, match="must hold a JSON list"):
        _ = serializer().decode(encoded)


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


def test_encoding_a_message_no_codec_handles_fails_loudly() -> None:
    with pytest.raises(MessageEncodingFailedError, match="no codec") as excinfo:
        _ = JsonSerializer().encode(Envelope("just a string"))

    assert excinfo.value.message_name == "builtins:str"


def test_decoding_a_type_no_codec_handles_fails_loudly() -> None:
    encoded = EncodedEnvelope(body="{}", headers={TYPE_HEADER: "builtins:str"})

    with pytest.raises(MessageDecodingFailedError, match="no codec"):
        _ = JsonSerializer().decode(encoded)


def test_the_first_codec_that_supports_the_type_is_used() -> None:
    first, second = SpyCodec(IngestDocument), SpyCodec(IngestDocument)

    _ = JsonSerializer(codecs=[first, second]).encode(Envelope(ingest_document()))

    assert first.encoded == 1
    assert second.encoded == 0


def test_the_wire_header_names_are_pinned() -> None:
    """These strings are the wire contract; a round-trip test would stay green
    even if they changed, so only literals catch a rename."""
    envelope = Envelope(ingest_document()).with_stamps(TransportMessageIdStamp("abc"))

    headers = serializer().encode(envelope).headers

    assert "type" in headers
    assert "X-Message-Stamp-TransportMessageIdStamp" in headers
