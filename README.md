# xtr-message-bus

A [Symfony Messenger](https://symfony.com/doc/current/messenger.html)-style message bus for Python.

Dispatch wraps a message in an **envelope** and walks it through a **middleware chain**.
MiddlewareInterface records what it did by appending a **stamp**. **Routing** maps a message type to
one or more named **transports**.

Transports that need a driver live behind an extra.

```bash
uv add xtr-message-bus                    # core: sync + in-memory transports
uv add "xtr-message-bus[pydantic]"        # message validation
uv add "xtr-message-bus[amqp]"            # RabbitMQ via taskiq
```

Requires Python 3.11+.

## Quickstart

A message is a plain frozen dataclass carrying identifiers. `@as_message` declares it, and its
properties live on the class:

```python
from dataclasses import dataclass
from uuid import UUID

from message_bus import as_message


@as_message(name="ingest.document.v1")
@dataclass(frozen=True, slots=True)
class IngestDocument:
    document_id: UUID
    tenant_id: UUID
```

`name` is the contract between producer and consumer, and the only thing they need to share.
It defaults to `module:QualName`, which changes if the class moves — pin an explicit,
versioned name for anything that outlives a deploy.

`transport` declares where the message goes when no routing table entry matches, so a message
can carry a sane default instead of every application repeating it:

```python
@as_message(name="ingest.document.v1", transport="async")
@dataclass(frozen=True, slots=True)
class IngestDocument: ...
```

Compose the bus once, at startup:

```python
from message_bus import (
    HandlersLocator,
    MessageBus,
    SendersLocator,
    SendMessageMiddleware,
    SyncTransport,
)

registry = HandlersLocator()
table = SendersLocator({IngestDocument: "sync"}, {"sync": SyncTransport(registry)})
bus = MessageBus([SendMessageMiddleware(table)])
```

Publish from anywhere that can reach the bus:

```python
await bus.dispatch(IngestDocument(document_id=doc.id, tenant_id=tenant.id))
```

Moving that message onto a real queue later is a change to the routing table. Nothing at the
dispatch site changes.

## Configuring transports

Configuration is inert data describing which transports exist and where messages go — the
Python reading of Symfony's `framework.messenger` section. It builds nothing; a
`MessageBusFactory` does that.

```python
from message_bus import MessageBusConfig, TransportConfig

CONFIG = MessageBusConfig(
    transports={
        "high": TransportConfig(AMQP_URL, queue="jobs_high"),
        "low": TransportConfig(f"{AMQP_URL}?queue=jobs_low"),
        "sync": TransportConfig("sync://"),
        "test": TransportConfig("in-memory://?serialize=true"),
    },
    routing={
        UrgentJob: "high",
        AuditRecorded: ["low", "test"],   # fan out
        "*": "low",                        # catch-all
    },
)

bus = MessageBusFactory(CONFIG).bus()
```

Two factories build from it, because a publishing process and a worker are different
deployments: `MessageBusFactory(config).bus()` and `WorkerFactory(config).worker([...])`.
Each has exactly one public method. You change *what* they build by giving them different
transport factories, not by reaching into the steps.

A DSN selects the adapter and carries its options. Transports that differ only in their query
string address the same server, so they share one connection.

| DSN | Transport |
|---|---|
| `sync://` | Handles in the calling process |
| `in-memory://` | Records; `?serialize=true` round-trips through the serializer |
| `amqp://…`, `amqps://…` | RabbitMQ via taskiq; `?queue=…` names the queue |

## Running work in a worker

Handlers are declared where the business logic lives, importing no broker:

```python
# app/handlers/ingest.py
from message_bus import as_message_handler
from app.messages import IngestDocument


@as_message_handler(IngestDocument)
async def ingest(message: IngestDocument) -> None: ...
```

The worker entrypoint names the transports it serves, and is an ordinary Python program:

```python
# app/worker_high.py
import asyncio

import app.handlers.ingest  # noqa: F401 — importing declares the handler

from app.bus import CONFIG
from message_bus import WorkerFactory

worker = WorkerFactory(CONFIG).worker(["high"])   # consumes jobs_high, nothing else

asyncio.run(worker.run())
```

```bash
uv run python -m app.worker_high
```

What comes back is a `WorkerInterface`, never the broker underneath — so the entrypoint sets up
logging and configuration like the rest of your application, and does not change if the transport
is swapped for one built on something other than taskiq.

Run one process per workload by pointing each at different transports — `WorkerFactory(CONFIG).worker(["low"])`
gives a worker that never sees `jobs_high`. The producer, meanwhile, gets one broker that knows
every queue it publishes to.

The producer never imports `app.handlers`: the task is addressed by the message's name, so
there is no route table to keep in sync and nothing to drift.

Check at startup that everything you publish has somewhere to land. Publishing to AMQP does
**not** fail for an unregistered name — the message is accepted and silently never consumed:

```python
assert_routes_registered(worker.broker, [IngestDocument, AnalyseDocument])
```

`worker.broker` is the taskiq broker the AMQP worker wraps, exposed for inspection like this.
Application code should depend on `WorkerInterface` instead — that is what keeps it free of
taskiq.

### Retries and dead-lettering

The AMQP transport wires a retry ladder and real dead-lettering. When attempts run out the
message is republished to `taskiq.dlq` in its original wire format, so it can be inspected and
replayed. (taskiq alone acknowledges an exhausted message, which means RabbitMQ never
dead-letters it and the work is simply lost.)

A handler can see which attempt it is on:

```python
from message_bus import Envelope, RedeliveryStamp


@as_message_handler(IngestDocument)
async def ingest(message: IngestDocument, envelope: Envelope) -> None:
    attempt = envelope.last(RedeliveryStamp)
    is_final = attempt is not None and attempt.retry_count >= MAX_ATTEMPTS - 1
```

## Validating messages

Dataclass messages are checked for **shape**: a missing, unexpected, or wrongly typed field
raises `MessageDecodingFailedError` at the boundary rather than arriving half-built in a handler.
Decoding never coerces — `"3"` is not accepted where an `int` is declared.

For rules a type cannot express, model the message with pydantic. Install the extra and it is
picked up automatically; both styles work on the same bus.

```python
from typing import Annotated, ClassVar

from pydantic import BaseModel, ConfigDict, Field


@as_message(name="billing.issue_invoice.v1")
class IssueInvoice(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True, extra="forbid")

    invoice_id: UUID
    amount: Annotated[Decimal, Field(gt=0)]
```

Validation runs on decode, where untrusted input arrives. Failures surface as
`MessageDecodingFailedError` — callers never import pydantic to catch them.

Strictness stays the model's own business: set `ConfigDict(strict=True)` if you want it.
Forcing it would break validators written to normalise their input.

Supported dataclass field types: `str`, `int`, `float`, `bool`, `None`, `UUID`, `datetime`,
`date`, `Decimal`, `Enum`, `list[T]`, `tuple[T, ...]`, `dict[str, T]`, `T | None`, and nested
dataclasses. Anything richer belongs in a pydantic model or a custom `SerializerInterface`.

> Message annotations are resolved at runtime. If you lint with ruff, set
> `runtime-evaluated-decorators = ["dataclasses.dataclass"]` so field imports are not moved
> into `TYPE_CHECKING` blocks.

## Adding behaviour

Every dispatch-side concern is middleware, so adding one changes no existing code:

```python
class RejectOutOfHours:
    async def handle(self, envelope, stack, /):
        if not within_business_hours():
            return envelope  # short-circuit: nothing downstream runs
        return await stack.next().handle(envelope, stack)


bus = MessageBus([LoggingMiddleware(), RejectOutOfHours(), SendMessageMiddleware(table)])
```

Routing resolves most specific first: a `TransportNamesStamp` on the envelope, then the
routing table walking the message's bases, then the `"*"` catch-all, then whatever the message
declared via `@as_message(transport=...)`. The table always wins over the message's own
declaration, so an application can re-route a message it does not own.

Two rules carry the producer/consumer split, and they are worth knowing:

- An envelope that arrived **from** a transport carries a `ReceivedStamp` and is never routed
  again, so a consumer cannot re-publish what it consumes.
- Once a sender accepts an envelope, the chain **short-circuits**. A message with a transport
  configured is handed off, not also handled locally.

## Transports

| Transport | Lives in | Needs | Use for |
|---|---|---|---|
| `SyncTransport` | `transport/sync/` | — | Handling in-process; local development |
| `InMemoryTransport` | `transport/in_memory/` | — | Tests: records what was dispatched, and can be consumed |
| `TaskiqSender` | `bridge/taskiq/` | `[taskiq]` | Publishing via **any** taskiq broker |
| `TaskiqWorker` | `bridge/taskiq/` | `[taskiq]` | Consuming via taskiq's own worker |
| `create_amqp_broker` | `bridge/amqp/` | `[amqp]` | RabbitMQ, with retries and dead-lettering |

Each transport is a package holding the transport and its factory, as Symfony groups
`Transport/Sync/` and `Transport/InMemory/`.

The tree encodes what each piece costs to import:

```
transport/          no dependency at all — sync, in_memory, the contracts
bridge/taskiq/      [taskiq]  — broker-agnostic: publish, consume, bind handlers
bridge/amqp/        [amqp]    — RabbitMQ only: connection, retry ladder, dead-lettering
```

`transport/` versus `bridge/` is the line Symfony draws by keeping AMQP in
`Bridge/Amqp/Transport/` rather than beside the others: **everything under `transport/` imports
with no optional dependency installed**, so an application speaking only `sync://` never pays
to import a broker library.

`bridge/taskiq/` versus `bridge/amqp/` is the same idea one level down. Nothing in the taskiq
layer names a broker driver, so `[taskiq]` is installable and usable on its own — a sender and
worker over Redis, NATS or an in-memory broker need no RabbitMQ. The dependency runs one way,
AMQP onto taskiq and never back, so a second broker is a package beside `bridge/amqp/` and no
change within it. Tests enforce all three claims.

`InMemoryTransport(serializer=JsonSerializer())` round-trips every message through
encode/decode on the way in, which catches an unserializable field in a unit test rather than
in production.

### The two halves

A transport sends, receives, or both. The contracts mirror Symfony's:

```python
class SenderInterface(Protocol):
    async def send(self, envelope: Envelope) -> Envelope: ...

class ReceiverInterface(Protocol):
    def get(self) -> AsyncIterator[Envelope]: ...
    async def ack(self, envelope: Envelope) -> None: ...
    async def reject(self, envelope: Envelope) -> None: ...

class TransportInterface(SenderInterface, ReceiverInterface, Protocol): ...
```

`get` is an async iterator rather than Symfony's polled `iterable`. Symfony polls because PHP
has no persistent async runtime to hold a subscription open; Python does, so this maps onto how
brokers actually deliver, and cancelling the task is graceful shutdown.

Adding a transport means implementing `TransportFactoryInterface`:

```python
class TransportFactoryInterface(Protocol):
    def supports(self, dsn: Dsn) -> bool: ...
    def create(self, group: Mapping[str, TransportConfig]) -> Mapping[str, SenderInterface]: ...
    def worker(self, group: Mapping[str, TransportConfig], bus: MessageBusInterface) -> WorkerInterface: ...
```

`create` builds the sending half; `worker` builds what a worker process runs. `worker` never
returns `None` — a transport with no backlog returns a worker that finishes immediately, so an
entrypoint can name any mix of transports without special-casing.

How the worker consumes is the adapter's business. Drive `Worker` over your own receive half,
or wrap a worker your broker library already provides, as the AMQP adapter does with taskiq's.
Both present the same `WorkerInterface`.

Pass your own to `MessageBusFactory(config, factories=[...])`.

## Replacing what the library composes

Nothing is welded shut. Every collaborator the library would otherwise pick for you is a
constructor argument, and the assembly steps are public.

```python
# a handlers locator of your own, instead of the process-wide default
MessageBusFactory(config, [SyncTransportFactory(handlers=my_locator)]).bus()

# your serializer, everywhere a transport encodes
MessageBusFactory(config, [AmqpTransportFactory(serializer=my_serializer)]).bus()

# your own scheme, alongside the ones that ship
MessageBusFactory(config, [MyTransportFactory(), *default_factories()]).bus()

# a worker driving your own receive half
Worker(my_bus, my_receiver)

# or skip the convenience entirely and compose everything yourself
MessageBus([*my_middleware, SendMessageMiddleware(SendersLocator(routing, senders), require_sender=True)])
```

| Want to replace | How |
|---|---|
| Where handlers are looked up | `SyncTransportFactory(handlers=...)`, `AmqpTransportFactory(handlers=...)` |
| How messages are encoded | `JsonSerializer(codecs=...)`, or any `SerializerInterface` passed to a factory |
| How one message type is encoded | Your own `MessageCodecInterface` in `JsonSerializer(codecs=[...])` |
| Where a message goes | `SendersLocator`, or any `SendersLocatorInterface` |
| How the bus is assembled | Build `SendersLocator` + `MessageBus` directly |
| A transport, or a whole scheme | Your own `TransportInterface` / `TransportFactoryInterface` |
| What a worker process runs | Your own `WorkerInterface`, or `Worker(bus, receiver)` |
| Retry and dead-letter policy | `AmqpTransportFactory(reliability=Reliability(...))` |
| The middleware chain | `MessageBus([...])` directly |

The factories and `default_factories()` are conveniences layered on those pieces, not gates
in front of them — every one is constructible on its own.

> **A private handlers registry has to go to the factory, not the bus.**
> `MessageBusFactory(config, handlers=...)` and `WorkerFactory(config, handlers=...)` configure
> the *bus*. Transports that resolve handlers themselves — `sync://`, and the AMQP adapter,
> which registers them with the broker — are built by discovery with no arguments, so they read
> the process-wide registry regardless. Pass the registry to the factory instead:
> `WorkerFactory(config, [AmqpTransportFactory(handlers=private)])`. For AMQP,
> `assert_routes_registered` catches the mistake at startup.

## Mapping to Symfony Messenger

| Symfony | Here |
|---|---|
| `MessageBusInterface`, `MessageBus` | `MessageBusInterface`, `MessageBus` |
| `Envelope`, `StampInterface` | `Envelope`, `StampInterface` |
| `NonSendableStampInterface` | `NonSendableStampInterface` |
| `MiddlewareInterface`, `StackInterface` | `MiddlewareInterface`, `StackInterface` |
| `SenderInterface` | `SenderInterface` |
| `ReceiverInterface` | `ReceiverInterface` (`get` is an async iterator) |
| `TransportInterface` | `TransportInterface` |
| `SendersLocatorInterface` | `SendersLocatorInterface`, `SendersLocator` |
| `#[AsMessage(transport: ...)]` | `@as_message(name=..., transport=...)` |
| `SendMessageMiddleware` | `SendMessageMiddleware` |
| `HandleMessageMiddleware` | `HandleMessageMiddleware` |
| `SerializerInterface` | `SerializerInterface`, `JsonSerializer` |
| `Symfony\Component\Messenger\Transport\Serialization` | `transport/serialization/` |
| `#[AsMessageHandler]` | `@as_message_handler` |
| `sync://`, `in-memory://` | `SyncTransport`, `InMemoryTransport` |
| `TransportFactoryInterface` | `TransportFactoryInterface` |
| `framework.messenger` config | `MessageBusConfig`, `TransportConfig` |
| `Worker` | `Worker`, `WorkerInterface` |
| `messenger:consume <transports>` | `WorkerFactory.worker([...])`, then `await worker.run()` |

Stamps shipped, one class per module under `message_bus/stamp/`: `BusNameStamp`, `SentStamp`, `TransportMessageIdStamp`, `DelayStamp`,
`RedeliveryStamp`, `ReceivedStamp`, `HandledStamp`, `ErrorDetailsStamp`, `TransportNamesStamp`, `AckReceiptStamp`.

### Who retries

The `Worker` loop makes **exactly one attempt per message** and never retries. Redelivery is
something only a transport can do correctly — it owns the delivery count, the backoff state and
the dead-letter destination — so a message that fails is rejected and the transport decides what
happens next. A retry in the loop would not replace that, it would run underneath it, and every
failure would be attempted the product of both policies.

A failure is not lost either way: the rejected envelope carries an `ErrorDetailsStamp` saying
what went wrong, so whatever the transport does with a rejection carries the reason with it.
One undeliverable message does not stop the worker.

### Deliberately not here yet

The full `HandlersLocator` with priorities and per-transport filtering; retry strategies as a
library concern (the broker owns them); a failure transport beyond the DLQ; batch handlers;
message deduplication; and multiple buses.

## Development

```bash
uv sync --all-extras
uv run ruff check src tests
uv run ruff format --check src tests
uv run basedpyright src tests
uv run pytest
```
