"""Records that a handler ran, and what it returned."""

from __future__ import annotations

from dataclasses import dataclass

from .non_sendable_stamp_interface import NonSendableStampInterface

__all__ = ["HandledStamp"]


@dataclass(frozen=True, slots=True)
class HandledStamp(NonSendableStampInterface):
    """Records that a handler ran, and what it returned.

    ``result`` is the handler's return value as it came back — never sent
    anywhere, so it need not be serializable. ``None`` for a handler that
    returns nothing.
    """

    handler_name: str
    result: object = None
