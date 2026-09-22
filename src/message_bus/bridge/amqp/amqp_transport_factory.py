"""Building RabbitMQ transports, and scoping a worker to some of them.

A producer needs one broker that knows every queue it publishes to. A worker
needs a broker that consumes *only* its own queue, so one deployment can run
a process per workload instead of one process draining everything.

Both come from the same spec:

* :meth:`AmqpTransportFactory.create` builds the producer side — one broker
  per connection, declaring every queue named on it.
* :meth:`AmqpTransportFactory.worker` builds the consumer side — a runnable
  worker declaring only the queues it was asked to serve, wrapping taskiq's
  own worker so nothing outside this module depends on taskiq.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, final

from taskiq_aio_pika import AioPikaBroker
from typing_extensions import override

from message_bus.bridge.taskiq.binding import bind_handlers
from message_bus.bridge.taskiq.taskiq_sender import TaskiqSender
from message_bus.bridge.taskiq.taskiq_worker import TaskiqWorker
from message_bus.exception import MixedDsnError
from message_bus.transport.serialization import JsonSerializer
from message_bus.transport.transport_factory_interface import TransportFactoryInterface
from message_bus.transport.transport_options import reject_unknown_options
from message_bus.worker_providing_interface import WorkerProvidingInterface

from .amqp_broker import create_amqp_broker
from .amqp_options import AMQP_OPTIONS, AmqpOptions

if TYPE_CHECKING:
    from collections.abc import Mapping

    from taskiq_aio_pika import AioPikaBroker

    from message_bus.dsn import Dsn
    from message_bus.handler import HandlersLocatorInterface
    from message_bus.message_bus_interface import MessageBusInterface
    from message_bus.transport.sender import SenderInterface
    from message_bus.transport.serialization import SerializerInterface
    from message_bus.transport.transport_config import TransportConfig
    from message_bus.worker_interface import WorkerInterface

__all__ = ["AMQP_SCHEMES", "AmqpTransportFactory", "MixedDsnError"]

AMQP_SCHEMES = frozenset({"amqp", "amqps"})


@final
class AmqpTransportFactory(TransportFactoryInterface, WorkerProvidingInterface):
    """Builds ``amqp://`` transports, sharing one broker per DSN."""

    __slots__ = ("_handlers", "_options", "_serializer")

    def __init__(
        self,
        options: AmqpOptions | None = None,
        serializer: SerializerInterface | None = None,
        handlers: HandlersLocatorInterface | None = None,
    ) -> None:
        """Apply these to everything it builds.

        ``handlers`` is only consulted when building a consumer, and defaults
        to the registry ``@as_message_handler`` fills.
        """
        self._options = options if options is not None else AmqpOptions()
        self._serializer = serializer
        self._handlers = handlers

    @override
    def supports(self, dsn: Dsn) -> bool:
        """Recognise the AMQP schemes."""
        return dsn.scheme in AMQP_SCHEMES

    @override
    def create(self, group: Mapping[str, TransportConfig]) -> Mapping[str, SenderInterface]:
        """Build one broker for the group, and a sender per named queue."""
        broker = self._broker_for(group)
        queues = _queues_of(group)
        return {
            name: TaskiqSender(
                broker,
                serializer=self._wire(),
                queue=spec.queue_name if queues else None,
            )
            for name, spec in group.items()
        }

    @override
    def worker(
        self,
        group: Mapping[str, TransportConfig],
        bus: MessageBusInterface,
    ) -> WorkerInterface:
        """Build a runnable worker consuming only ``group``.

        The declared handlers are registered on the broker here, so the
        result needs no further wiring — ``await worker.run()`` and it
        consumes. It listens on exactly these queues, so a worker serving a
        different transport is unaffected.

        Import the modules that declare your handlers before calling this;
        a handler that has not been declared cannot be bound.

        ``bus`` is deliberately unused. taskiq resolves a message to a
        registered task itself, and that task already invokes the handler, so
        routing the message through a bus as well would run the middleware
        chain twice over the same message. Transports without a worker of
        their own dispatch through ``bus`` instead — the parameter is part of
        the contract, not of every implementation of it.

        Raises:
            MixedDsnError: If the transports do not share one connection.
        """
        del bus
        broker = self._broker_for(group)
        _ = bind_handlers(broker, self._handlers, self._wire())
        return TaskiqWorker(broker)

    def _wire(self) -> SerializerInterface:
        """Return the serializer both halves of this transport use.

        Producer and consumer are separate processes, so the guarantee that
        matters is that an un-customised deploy is symmetric by
        construction: both sides reach this method and get the same
        configuration. Passing ``serializer`` overrides both at once, never
        one of them.
        """
        if self._serializer is None:
            self._serializer = JsonSerializer()
        return self._serializer

    def _options_for(self, group: Mapping[str, TransportConfig]) -> AmqpOptions:
        """Read the settings every transport in ``group`` carries.

        They share one connection, so they share one broker and therefore one
        set of options. A later transport overrides an earlier one rather
        than silently disagreeing with it.

        Raises:
            UnknownTransportOptionError: If a setting is not one this
                transport accepts.
        """
        merged: dict[str, str] = {}
        for spec in group.values():
            reject_unknown_options(spec.parsed.scheme, spec.settings, AMQP_OPTIONS)
            merged.update(spec.settings)
        return AmqpOptions.from_settings(merged, self._options)

    def _broker_for(self, group: Mapping[str, TransportConfig]) -> AioPikaBroker:
        """Return the broker for ``group``'s connection.

        Not shared between groups: the queues a group names are declared on
        the broker, so a worker serving one queue and a producer publishing
        to several need brokers that differ in what they declare.

        Raises:
            MixedDsnError: If the transports do not share one connection.
        """
        connections = tuple(dict.fromkeys(spec.parsed.connection for spec in group.values()))
        if len(connections) != 1:
            raise MixedDsnError(connections)
        return create_amqp_broker(connections[0], self._options_for(group), _queues_of(group))


def _queues_of(group: Mapping[str, TransportConfig]) -> tuple[str, ...]:
    named = (spec.queue_name for spec in group.values())
    return tuple(dict.fromkeys(q for q in named if q is not None))
