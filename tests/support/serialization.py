"""The envelope serializer the suite carries messages with."""

from __future__ import annotations

from typing import TYPE_CHECKING

from xtr_serializer import JsonEncoder, PydanticNormalizer, Serializer

from xtr_messenger import JsonSerializer

if TYPE_CHECKING:
    from collections.abc import Iterable

    from xtr_messenger import StampInterface


def json_serializer(stamp_types: Iterable[type[StampInterface]] | None = None) -> JsonSerializer:
    """Build a JSON envelope serializer over a fresh xtr-serializer chain."""
    return JsonSerializer(Serializer([PydanticNormalizer()], [JsonEncoder()]), stamp_types)
