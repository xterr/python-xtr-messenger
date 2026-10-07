"""Requests delayed delivery, in milliseconds."""

from __future__ import annotations

from dataclasses import dataclass

from .stamp_interface import StampInterface

__all__ = ["DelayStamp"]


@dataclass(frozen=True, slots=True)
class DelayStamp(StampInterface):
    """Requests delayed delivery, in milliseconds.

    Honoured only by a transport that can hold a message before delivering
    it. The AMQP transport does, through taskiq's delayed-message exchange.
    The ``sync://`` and ``in-memory://`` transports cannot — they hand a
    message on at once — and refuse a delayed envelope with
    :class:`~xtr_messenger.exception.UnsupportedStampError` rather than
    delivering it immediately and dropping the request silently.
    """

    delay_ms: int
