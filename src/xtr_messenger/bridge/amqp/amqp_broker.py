"""Moved to :mod:`xtr_amqp_messenger.transport.amqp_broker`; kept for this major version."""

from __future__ import annotations

from xtr_amqp_messenger.transport.amqp_broker import create_amqp_broker, declared_queues

__all__ = ["create_amqp_broker", "declared_queues"]
