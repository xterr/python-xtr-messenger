"""Building RabbitMQ transports, and scoping a worker to some of them.

A producer needs one broker that knows every queue it publishes to. A worker
needs a broker that consumes *only* its own queue, so one deployment can run
a process per workload instead of one process draining everything.

Both come from the same configuration:

* :meth:`AmqpTransportFactory.create` builds the producer side — one broker
  per connection, declaring every queue named on it.
* :meth:`AmqpTransportFactory.worker` builds the consumer side — a runnable
  worker declaring only the queues it was asked to serve, wrapping taskiq's
  own worker so nothing outside this module depends on taskiq.

Only the producer side leaves a connection behind: a worker's run loop closes
its own broker, while a publisher has no loop to do that, so this factory keeps
track of what its senders opened and :meth:`AmqpTransportFactory.close_publishers`
releases it.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Final, final

from typing_extensions import override

from xtr_messenger.bridge.taskiq.binding import bind_bus
from xtr_messenger.bridge.taskiq.started_brokers import StartedBrokers
from xtr_messenger.bridge.taskiq.taskiq_sender import TaskiqSender
from xtr_messenger.bridge.taskiq.taskiq_worker import TaskiqWorker
from xtr_messenger.exception import MixedDsnError
from xtr_messenger.publisher_closing_interface import PublisherClosingInterface
from xtr_messenger.transport.serialization import JsonSerializer
from xtr_messenger.transport.transport_factory_interface import TransportFactoryInterface
from xtr_messenger.transport.transport_options import reject_unknown_options
from xtr_messenger.worker_providing_interface import WorkerProvidingInterface

from .amqp_broker import create_amqp_broker
from .amqp_options import AMQP_OPTIONS, AmqpOptions
from .dead_lettering import DETAIL_LIMIT, publish_dead_letter

if TYPE_CHECKING:
    from collections.abc import Mapping

    from taskiq import TaskiqMessage
    from taskiq_aio_pika import AioPikaBroker
    from xtr_event_dispatcher_contracts import EventDispatcherInterface

    from xtr_messenger.bridge.taskiq.binding import UndecodableDeadLetterer
    from xtr_messenger.bridge.taskiq.poison_message_receiver import DeadLetterer
    from xtr_messenger.dsn import Dsn
    from xtr_messenger.exception import MessageDecodingFailedError
    from xtr_messenger.message_bus_interface import MessageBusInterface
    from xtr_messenger.transport.sender import SenderInterface
    from xtr_messenger.transport.serialization import SerializerInterface
    from xtr_messenger.transport.transport_config import TransportConfig
    from xtr_messenger.worker import AsyncResetter
    from xtr_messenger.worker_interface import WorkerInterface

__all__ = ["AMQP_SCHEMES", "AmqpTransportFactory", "MixedDsnError"]

AMQP_SCHEMES = frozenset({"amqp", "amqps"})

#: Warnings go through the messenger channel, so an application capturing the
#: standard library's logging sees them beside the rest of the bus's output.
_LOGGER: Final = logging.getLogger("messenger")

#: Logged when TLS identity checking is turned off — a security downgrade
#: that must not be silent, warned once per factory rather than on every
#: broker built from the same configuration.
_VERIFY_OFF_WARNING: Final = (
    "TLS certificate verification is disabled (verify=false): the broker's identity is not checked"
)


@final
class AmqpTransportFactory(
    TransportFactoryInterface, WorkerProvidingInterface, PublisherClosingInterface
):
    """Builds ``amqp://`` transports, sharing one broker per DSN."""

    __slots__ = ("_options", "_serializer", "_started", "_verify_warned")

    def __init__(
        self,
        options: AmqpOptions | None = None,
        serializer: SerializerInterface | None = None,
    ) -> None:
        """Apply these to everything it builds."""
        self._options = options if options is not None else AmqpOptions()
        self._serializer = serializer
        # This factory's own publish connections, never another factory's.
        self._started = StartedBrokers()
        self._verify_warned = False

    @override
    def supports(self, dsn: Dsn) -> bool:
        """Recognise the AMQP schemes."""
        return dsn.scheme in AMQP_SCHEMES

    @override
    def create(self, group: Mapping[str, TransportConfig]) -> Mapping[str, SenderInterface]:
        """Build one broker for the group, and a sender per named queue."""
        options = self._options_for(group)
        broker = self._broker_for(group, options)
        queues = _queues_of(group)
        return {
            name: TaskiqSender(
                broker,
                self._started,
                serializer=self._wire(),
                queue=transport.queue_name if queues else None,
                supports_delay=options.delayed_message_exchange_plugin,
            )
            for name, transport in group.items()
        }

    @override
    async def close_publishers(self) -> None:
        """Close every connection this factory's senders opened on first publish.

        A worker's broker is closed by its own run loop and was never recorded,
        so this releases the producer side only.
        """
        await self._started.close_all()

    @override
    def worker(
        self,
        group: Mapping[str, TransportConfig],
        bus: MessageBusInterface,
        *,
        event_dispatcher: EventDispatcherInterface | None = None,
        resetter: AsyncResetter | None = None,
    ) -> WorkerInterface:
        """Build a runnable worker consuming only ``group``.

        Every declared message is registered on the broker as a task that
        dispatches into ``bus``, so the result needs no further wiring —
        ``await worker.run()`` and it consumes, with ``bus`` deciding which
        handlers run. It listens on exactly these queues, so a worker serving
        a different transport is unaffected.

        Import the modules that declare your messages and handlers before
        calling this; a message that has not been declared cannot be bound.
        It handles at most ``max_async_tasks`` messages at once.

        ``event_dispatcher`` hears the worker events, each message reported
        as received from the transport whose queue it arrived on.

        ``resetter`` has ``reset()`` awaited after each message, settled
        either way, so long-lived services are cleared between units of work.

        With a dead-letter queue configured, a delivery that can never be
        handled — unparsable, naming a task nobody registered, or whose body
        does not decode into its message — is quarantined there at once
        rather than retried.

        Raises:
            MixedDsnError: If the transports do not share one connection.
        """
        options = self._options_for(group)
        broker = self._broker_for(group, options)
        dead_letter_queue = options.reliability.dead_letter_queue
        _ = bind_bus(
            broker,
            bus,
            self._wire(),
            event_dispatcher=event_dispatcher,
            receiver_names={transport.queue_name: name for name, transport in group.items()},
            max_attempts=options.reliability.max_attempts,
            resetter=resetter,
            dead_letter=(
                _undecodable_dead_letterer(broker, dead_letter_queue)
                if dead_letter_queue is not None
                else None
            ),
        )
        return TaskiqWorker(
            broker,
            max_async_tasks=options.max_async_tasks,
            event_dispatcher=event_dispatcher,
            dead_letter=(
                _poison_dead_letterer(broker, dead_letter_queue)
                if dead_letter_queue is not None
                else None
            ),
        )

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
        for transport in group.values():
            reject_unknown_options(transport.parsed.scheme, transport.settings, AMQP_OPTIONS)
            merged.update(transport.settings)
        options = AmqpOptions.from_settings(merged, self._options)
        if not options.connection.verify and not self._verify_warned:
            self._verify_warned = True
            _LOGGER.warning(_VERIFY_OFF_WARNING)
        return options

    def _broker_for(
        self, group: Mapping[str, TransportConfig], options: AmqpOptions
    ) -> AioPikaBroker:
        """Return the broker for ``group``'s connection, configured by ``options``.

        Not shared between groups: the queues a group names are declared on
        the broker, so a worker serving one queue and a producer publishing
        to several need brokers that differ in what they declare.

        Raises:
            MixedDsnError: If the transports do not share one connection.
        """
        connections = tuple(
            dict.fromkeys(transport.parsed.connection for transport in group.values())
        )
        if len(connections) != 1:
            raise MixedDsnError(connections)
        return create_amqp_broker(connections[0], options, _queues_of(group))


def _queues_of(group: Mapping[str, TransportConfig]) -> tuple[str, ...]:
    named = (transport.queue_name for transport in group.values())
    return tuple(dict.fromkeys(q for q in named if q is not None))


def _undecodable_dead_letterer(broker: AioPikaBroker, queue_name: str) -> UndecodableDeadLetterer:
    """Quarantine a delivery whose body does not decode into its message."""

    async def dead_letter(message: TaskiqMessage, error: MessageDecodingFailedError) -> None:
        encoded = broker.formatter.dumps(message)
        await publish_dead_letter(
            broker,
            queue_name,
            encoded.message,
            {
                "task_id": message.task_id,
                "task_name": message.task_name,
                "x-death-reason": type(error).__name__,
                "x-death-detail": str(error)[:DETAIL_LIMIT],
                **message.labels,
            },
        )

    return dead_letter


def _poison_dead_letterer(broker: AioPikaBroker, queue_name: str) -> DeadLetterer:
    """Quarantine a delivery the receive loop cannot even parse."""

    async def dead_letter(body: bytes, reason: str) -> None:
        await publish_dead_letter(broker, queue_name, body, {"x-death-reason": reason})

    return dead_letter
