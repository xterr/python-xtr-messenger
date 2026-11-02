"""Moved to :mod:`xtr_amqp_messenger.transport.dead_lettering`; kept for this major version."""

from __future__ import annotations

from xtr_amqp_messenger.transport.dead_lettering import DETAIL_LIMIT, publish_dead_letter

__all__ = ["DETAIL_LIMIT", "publish_dead_letter"]
