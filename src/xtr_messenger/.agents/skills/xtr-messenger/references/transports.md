# Transports

| DSN | What it does | Needs |
| --- | --- | --- |
| `sync://` | Handles in the calling process, during the dispatch | — |
| `in-memory://` | Records what was dispatched; a worker or a test drains it. `?serialize=true` round-trips every message through the serializer | — |
| `amqp://…`, `amqps://…` | RabbitMQ, with a retry ladder and dead-lettering | `[amqp]` |

Any other taskiq broker goes through `[taskiq]` — `TaskiqSender` / `TaskiqWorker` under
`xtr_messenger.bridge.taskiq`. Nothing under `xtr_messenger.transport/` needs an extra;
anything needing a driver is a bridge.

## Writing settings

Settings go in the DSN query string or in `TransportConfig.options`. `options` wins; `queue=`
as a field wins over both.

```python
TransportConfig(
    "amqp://user:pass@rabbit:5672/?queue=jobs&max_attempts=10",
    options={"prefetch_count": "50", "dead_letter_queue": "jobs.dlq"},
)
```

Option values are strings. An unrecognised name raises `UnknownTransportOptionError`; a value
the adapter cannot use raises `InvalidTransportOptionError`. Credentials belong in the DSN, not
in `options`.

## What an `amqp://` transport accepts

| Group | Settings |
| --- | --- |
| Retries | `max_attempts`, `base_delay_seconds`, `dead_letter_queue` |
| Exchange | `exchange`, `exchange_type`, `exchange_durable`, `exchange_auto_delete` |
| Queue | `queue`, `queue_type`, `queue_durable`, `queue_auto_delete`, `queue_exclusive`, `queue_max_priority`, `routing_key` |
| Connection | `heartbeat`, `connect_timeout`, `connection_name`, `frame_max`, `channel_max` |
| TLS | `cacert`, `cert`, `key`, `verify` |
| Consumption | `prefetch_count`, `max_async_tasks`, `auto_setup` |

`max_async_tasks` is how many messages a worker handles at once (ten per CPU, capped at 100).
A message is acked once handled, so `prefetch_count` (10 by default) caps it too: a worker
handles the smaller of the two. Raise both together.

On AMQP a worker registers one task per message declared with `@as_message`, so import the
modules declaring your messages before building it.

## Who retries

The library's loop makes exactly one attempt per message and never retries. A failed message is
rejected carrying an `ErrorDetailsStamp`, and the transport decides what happens next. On AMQP
that means the retry ladder, then republishing to `taskiq.dlq` (or the `dead_letter_queue` you
name) in the original wire format. A handler asking for the `Envelope` reads the attempt from
`envelope.last(RedeliveryStamp)`.

## A transport of your own

Implement `TransportFactoryInterface` — `supports(dsn: Dsn) -> bool` and
`create(group: Mapping[str, TransportConfig]) -> Mapping[str, SenderInterface]` — and advertise
it, where the entry-point name is the DSN scheme:

```toml
[project.entry-points."xtr_messenger.transport_factories"]
kafka = "my_package.kafka:KafkaTransportFactory"
```

A sender that can also be consumed implements `TransportInterface` (`send`, `get`, `ack`,
`reject`) and the library's `Worker` drives it. Only implement `WorkerProvidingInterface` when
the broker owns its own consume loop.

Under a kernel, a class implementing `TransportFactoryInterface` in a scanned module is
registered automatically and consulted ahead of entry-point discovery, in registration order.
It must be buildable with no arguments. A collaborator a factory needs (a serializer, say)
cannot travel in a DSN — pass the factory yourself:
`MessageBusFactory(CONFIG, [AmqpTransportFactory(serializer=mine)]).bus()`.

## Receivers without a DSN

A service implementing `ReceiverInterface` and tagged `RECEIVER_TAG` (`"messenger.receiver"`)
with an `alias` is consumable under that alias with no entry in `transports`:

```python
from xtr_dependency_injection import as_service, autoconfigure

from xtr_messenger.bundle import RECEIVER_TAG


@autoconfigure(tags=[(RECEIVER_TAG, {"alias": "ticks"})])
@as_service
class TickReceiver(ReceiverInterface): ...
```

A configured transport of the same name wins. Two receivers under one alias fail the build.
Building a receiver must do no I/O — every tagged one is built with the `WorkerFactory`.
Without a container: `WorkerFactory(CONFIG, receivers={"ticks": TickReceiver()})`. One worker
may drain receivers and configured transports together, except a transport bringing its own
worker (`IncompatibleReceiversError`).
