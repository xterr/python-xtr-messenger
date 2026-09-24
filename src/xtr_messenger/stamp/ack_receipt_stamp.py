"""The local handle identifying a received message to its transport."""

from __future__ import annotations

from dataclasses import dataclass

from .non_sendable_stamp_interface import NonSendableStampInterface

__all__ = ["AckReceiptStamp"]


@dataclass(frozen=True, slots=True)
class AckReceiptStamp(NonSendableStampInterface):
    """Identifies a received message so it can later be acked or rejected.

    A receiver mints one of these as it yields an envelope, and keeps the
    means of acknowledging that message — a broker callback, a delivery tag —
    on its own side, keyed by this receipt. The envelope carries only the
    number.

    That indirection is deliberate. The acknowledging machinery is a live
    connection-bound object: it cannot be serialized, and it is meaningless
    in any other process. Keeping it off the envelope means a received
    message stays exactly as serializable as a sent one, so the same envelope
    can be forwarded, retried, or written to a dead-letter queue without the
    transport's private state leaking with it.

    Non-sendable, so it is dropped before encoding: a receipt describes this
    process's relationship to a message, never the message itself.
    """

    receipt: int
