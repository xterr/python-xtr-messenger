"""Where the RabbitMQ transport used to live: it is now the xtr-amqp-messenger package.

Kept so code importing from here keeps working through this major version;
every name is the very object :mod:`xtr_amqp_messenger.transport` defines.
Import from there instead — this module is removed in the next major version.
Needs xtr-amqp-messenger installed, as ``xtr-messenger[amqp]`` does.
"""

from __future__ import annotations

from xtr_amqp_messenger.transport import (
    AMQP_SCHEMES,
    DEFAULT_DEAD_LETTER_QUEUE,
    AmqpTransportFactory,
    DeadLetterMiddleware,
    MixedDsnError,
    Reliability,
    create_amqp_broker,
    declared_queues,
)

__all__ = [
    "AMQP_SCHEMES",
    "DEFAULT_DEAD_LETTER_QUEUE",
    "AmqpTransportFactory",
    "DeadLetterMiddleware",
    "MixedDsnError",
    "Reliability",
    "create_amqp_broker",
    "declared_queues",
]
