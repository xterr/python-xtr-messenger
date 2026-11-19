"""What a receiver that can count the messages waiting in it offers."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

__all__ = ["MessageCountAwareInterface"]


@runtime_checkable
class MessageCountAwareInterface(Protocol):
    """A receiver that can tell how many messages wait to be handled."""

    async def count(self) -> int:
        """Return how many messages wait to be handled; on some brokers an approximation."""
        ...
