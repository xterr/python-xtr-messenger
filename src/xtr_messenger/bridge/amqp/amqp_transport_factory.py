"""Moved to :mod:`xtr_amqp_messenger.transport.amqp_transport_factory`, for this major version."""

from __future__ import annotations

from xtr_amqp_messenger.transport.amqp_transport_factory import (
    AMQP_SCHEMES,
    AmqpTransportFactory,
    MixedDsnError,
)

__all__ = ["AMQP_SCHEMES", "AmqpTransportFactory", "MixedDsnError"]
