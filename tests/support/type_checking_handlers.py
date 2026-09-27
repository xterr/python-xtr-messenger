"""Handlers whose annotations name classes imported for type checking alone."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tests.support.messages import IngestDocument
    from xtr_messenger import Envelope

__all__ = ["wants_the_envelope", "wants_the_message"]


async def wants_the_envelope(message: IngestDocument, envelope: Envelope) -> None:
    """Asks for the envelope, with ``Envelope`` unknown at runtime."""
    del message, envelope


async def wants_the_message(message: IngestDocument) -> None:
    """Takes the message alone, its class unknown at runtime."""
    del message
