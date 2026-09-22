"""Choosing the codecs a serializer uses when given none."""

from __future__ import annotations

from typing import TYPE_CHECKING

from .dataclass_codec import DataclassCodec

if TYPE_CHECKING:
    from .message_codec_interface import MessageCodecInterface

__all__ = ["default_codecs"]


def default_codecs() -> tuple[MessageCodecInterface, ...]:
    """Return the codecs a serializer uses when none are given.

    Dataclasses are always handled. Pydantic models are handled too when
    pydantic is installed - a producer and a consumer that were configured
    separately then still agree, instead of the consumer failing to decode
    what the producer happily encoded.

    Detection cannot change behaviour: without pydantic installed there are
    no pydantic messages to carry.
    """
    try:
        from .pydantic_codec import PydanticCodec  # noqa: PLC0415
    except ImportError:
        return (DataclassCodec(),)
    return (PydanticCodec(), DataclassCodec())
