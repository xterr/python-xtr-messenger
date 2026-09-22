"""The contract for turning envelopes into something a transport can carry."""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from message_bus.envelope import Envelope

    from .encoded_envelope import EncodedEnvelope

__all__ = ["SerializerInterface"]


@runtime_checkable
class SerializerInterface(Protocol):
    """Converts envelopes to and from a transport-ready representation."""

    def encode(self, envelope: Envelope) -> EncodedEnvelope:
        """Render ``envelope`` for the wire."""
        ...

    def decode(self, encoded: EncodedEnvelope) -> Envelope:
        """Rebuild an envelope from its wire form."""
        ...
