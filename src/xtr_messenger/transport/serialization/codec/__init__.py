"""How one family of message types becomes JSON, and comes back."""

from __future__ import annotations

from .dataclass_codec import DataclassCodec
from .default_codecs import default_codecs
from .message_codec_interface import JsonValue, MessageCodecInterface

__all__ = ["DataclassCodec", "JsonValue", "MessageCodecInterface", "default_codecs"]
