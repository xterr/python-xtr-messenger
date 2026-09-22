r"""Turning envelopes into something a transport can carry, and back.

A :class:`SerializerInterface` owns the wire format — a body plus headers,
stamps included — while a :class:`MessageCodecInterface` owns how one family
of message types becomes JSON. That separation is what lets dataclass and
pydantic messages share a single bus.
"""

from .codec import DataclassCodec, JsonValue, MessageCodecInterface, default_codecs
from .encoded_envelope import EncodedEnvelope
from .serializer import TYPE_HEADER, JsonSerializer
from .serializer_interface import SerializerInterface

__all__ = [
    "TYPE_HEADER",
    "DataclassCodec",
    "EncodedEnvelope",
    "JsonSerializer",
    "JsonValue",
    "MessageCodecInterface",
    "SerializerInterface",
    "default_codecs",
]
