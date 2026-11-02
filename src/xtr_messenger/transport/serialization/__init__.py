r"""Turning envelopes into something a transport can carry, and back.

A :class:`SerializerInterface` owns the wire format — a body plus headers,
stamps included. :class:`JsonSerializer` writes the body and every stamp
through xtr-serializer, so dataclasses and pydantic models share one bus and
arrive exactly as they left.
"""

from __future__ import annotations

from .encoded_envelope import EncodedEnvelope
from .serializer import TYPE_HEADER, JsonSerializer
from .serializer_interface import SerializerInterface

__all__ = [
    "TYPE_HEADER",
    "EncodedEnvelope",
    "JsonSerializer",
    "SerializerInterface",
]
