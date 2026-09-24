"""The RabbitMQ bridge, built on the taskiq one.

Installed with the ``amqp`` extra, which brings the ``taskiq`` extra with it.
This package holds only what is genuinely RabbitMQ: the connection and its
queues, the retry ladder, and real dead-lettering. Publishing and consuming
themselves are broker-agnostic and live in
:mod:`xtr_messenger.bridge.taskiq`.

The dependency runs one way — AMQP builds on taskiq, never the reverse — so
a second taskiq broker needs a package beside this one and no change within
it.

Dead-lettering is the part worth knowing about: taskiq alone acknowledges a
message whose attempts have run out, so RabbitMQ never dead-letters it and
the work is simply lost.
:class:`~xtr_messenger.bridge.amqp.dead_letter_middleware.DeadLetterMiddleware`
republishes it instead, in its original wire format, so it can be inspected
and replayed.
"""

from .amqp_broker import create_amqp_broker, declared_queues
from .amqp_transport_factory import AMQP_SCHEMES, AmqpTransportFactory, MixedDsnError
from .dead_letter_middleware import DeadLetterMiddleware
from .reliability import DEFAULT_DEAD_LETTER_QUEUE, Reliability

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
