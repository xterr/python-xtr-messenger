"""Moved to :mod:`xtr_amqp_messenger.transport.reliability`; kept for this major version."""

from __future__ import annotations

from xtr_amqp_messenger.transport.reliability import (
    DEFAULT_DEAD_LETTER_QUEUE,
    RELIABILITY_OPTIONS,
    Reliability,
)

__all__ = ["DEFAULT_DEAD_LETTER_QUEUE", "RELIABILITY_OPTIONS", "Reliability"]
