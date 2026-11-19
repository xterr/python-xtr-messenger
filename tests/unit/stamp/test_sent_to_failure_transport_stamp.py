from __future__ import annotations

from datetime import UTC, datetime

from tests.support.messages import ingest_document
from tests.support.serialization import json_serializer
from xtr_messenger import Envelope, SentToFailureTransportStamp


def test_it_travels_with_the_message_by_default() -> None:
    serializer = json_serializer()
    stamp = SentToFailureTransportStamp("orders", datetime(2026, 1, 1, 12, tzinfo=UTC))

    decoded = serializer.decode(serializer.encode(Envelope(ingest_document(), (stamp,))))

    assert decoded.last(SentToFailureTransportStamp) == stamp
