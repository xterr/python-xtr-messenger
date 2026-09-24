"""An envelope in transport-ready form."""

from __future__ import annotations

from dataclasses import dataclass

__all__ = ["EncodedEnvelope"]


@dataclass(frozen=True, slots=True)
class EncodedEnvelope:
    """An envelope in transport-ready form."""

    body: str
    headers: dict[str, str]
