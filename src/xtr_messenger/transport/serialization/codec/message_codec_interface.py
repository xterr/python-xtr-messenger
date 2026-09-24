"""The contract for turning one family of message types into JSON.

Separating this from the serializer keeps the wire format — a body plus
headers, stamps included — independent of how a message is modelled. A
serializer holds a list of codecs and uses the first that recognises the
message type, so an application can model some messages as dataclasses and
others as pydantic models without running two buses.
"""

from __future__ import annotations

from typing import Protocol, TypeAlias, runtime_checkable

__all__ = ["JsonValue", "MessageCodecInterface"]

JsonValue: TypeAlias = "str | int | float | bool | list[JsonValue] | dict[str, JsonValue] | None"


@runtime_checkable
class MessageCodecInterface(Protocol):
    """Converts one family of message types to and from JSON."""

    def supports(self, message_type: type) -> bool:
        """Report whether this codec handles ``message_type``."""
        ...

    def encode(self, message: object) -> JsonValue:
        """Render ``message`` as a JSON structure.

        Implementations raise
        :class:`~xtr_messenger.exception.MessageEncodingFailedError` when
        ``message`` has no JSON form.
        """
        ...

    def decode(self, message_type: type, raw: JsonValue) -> object:
        """Rebuild a ``message_type`` instance from ``raw``.

        Implementations raise
        :class:`~xtr_messenger.exception.MessageDecodingFailedError` when
        ``raw`` does not describe a valid message, so callers catch one
        exception type whatever the codec.
        """
        ...
