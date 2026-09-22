<div align="center">

# xtr-message-bus

**A message bus for Python — envelopes, stamps, a middleware chain, and pluggable transports.**

<img alt="python 3.11+" src="https://img.shields.io/badge/python-%E2%89%A5%203.11-3776AB?logo=python&logoColor=white">
<img alt="core dependencies: 2" src="https://img.shields.io/badge/core%20deps-2-3FB950">
<img alt="typed" src="https://img.shields.io/badge/typed-ty%20%2B%20basedpyright-1f6feb">
<img alt="license MIT" src="https://img.shields.io/badge/license-MIT-blue">

</div>

---

## Why?

Publishing a message should not drag a broker into your import graph, and moving a message
onto a queue should not mean rewriting the code that sends it.

Dispatch wraps a message in an **envelope** and walks it through a **middleware chain**. Each
middleware records what it did by appending a **stamp**. A **routing table** maps a message
type to one or more named **transports**. Nothing at the dispatch site knows which.

- 🪶 **Two core dependencies** — `msgspec` and `typing-extensions`. A broker is an extra.
- 🔌 **Transports are discovered** — one entry point adds a scheme; no fork, no registry to edit.
- 💤 **Lazily loaded** — an app speaking `sync://` never imports a broker library.
- 🧩 **Protocol-based** — every collaborator is a constructor argument, so a DI container can own the graph.
- 📨 **Dataclasses or pydantic** — both on one bus, strict on the wire either way.

```python
await bus.dispatch(IngestDocument(document_id=doc.id))
```

Where that goes is configuration. Whether it happens in-process or on RabbitMQ is a DSN.

## Install

```sh
uv add xtr-message-bus                    # sync:// and in-memory://
uv add "xtr-message-bus[amqp]"            # + RabbitMQ
uv add "xtr-message-bus[pydantic]"        # + pydantic messages
```

| Extra | Brings | For |
| --- | --- | --- |
| *(none)* | `msgspec`, `typing-extensions` | `sync://`, `in-memory://`, the whole core |
| `pydantic` | `pydantic` | Messages validated by a model, not just a shape |
| `taskiq` | `taskiq` | Publishing and consuming over **any** taskiq broker |
| `amqp` | `taskiq-aio-pika` | RabbitMQ, with retries and real dead-lettering |

Requires Python 3.11+.

## Quick start

A message is a frozen dataclass carrying identifiers. `@as_message` declares it, and its
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

`name` is the contract between producer and consumer, and the only thing they share. It
defaults to `module:QualName`, which changes if the class moves — pin an explicit, versioned
name for anything that outlives a deploy.

A handler is a function that imports nothing but the message:

```python
from message_bus import as_message_handler


@as_message_handler(IngestDocument)
async def ingest(message: IngestDocument) -> None:
    ...
```

Describe the transports and where messages go, then build a bus:

```python
from message_bus import MessageBusConfig, MessageBusFactory, TransportConfig

CONFIG = MessageBusConfig(
    transports={"sync": TransportConfig("sync://")},
    routing={IngestDocument: "sync"},
)

bus = MessageBusFactory(CONFIG).bus()
await bus.dispatch(IngestDocument(document_id=doc_id, tenant_id=tenant_id))
```

Moving that message onto RabbitMQ later is one line of configuration. The dispatch site does
not change.

## Configuring transports

Configuration is inert data: which transports exist and where messages go. It builds nothing —
a factory does — so it can come from a settings module, an environment variable or a parsed
file without dragging a broker along.

```python
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
        "*": "low",                       # catch-all
    },
)
```

A DSN selects the adapter and carries its settings. Transports differing only in their query
string address the same server, so they share one connection.

| DSN | Transport |
| --- | --- |
| `sync://` | Handles in the calling process |
| `in-memory://` | Records what was dispatched; `?serialize=true` round-trips it |
| `amqp://…`, `amqps://…` | RabbitMQ via taskiq |

Settings go in the query string or in `options`, whichever suits — a DSN travels in one
environment variable, `options` survives review once there are several. `options` wins.

```python
TransportConfig(
    "amqp://user:pass@rabbit:5672/?queue=jobs&max_attempts=10",
    options={"prefetch_count": "50", "dead_letter_queue": "jobs.dlq"},
)
```

<details>
<summary><b>All 25 settings an <code>amqp://</code> transport accepts</b></summary>

| Group | Settings |
| --- | --- |
| Retries | `max_attempts`, `base_delay_seconds`, `dead_letter_queue` |
| Exchange | `exchange`, `exchange_type`, `exchange_durable`, `exchange_auto_delete` |
| Queue | `queue`, `queue_type`, `queue_durable`, `queue_auto_delete`, `queue_exclusive`, `queue_max_priority`, `routing_key` |
| Connection | `heartbeat`, `connect_timeout`, `connection_name`, `frame_max`, `channel_max` |
| TLS | `cacert`, `cert`, `key`, `verify` |
| Consumption | `prefetch_count`, `auto_setup` |

An unrecognised setting is **refused**, naming what the scheme does accept. A typo in
configuration is always a mistake, and one that is ignored leaves a transport running on
defaults nobody chose.

Credentials are not settings — a URL already expresses them, so they stay in the DSN.

</details>

## Running a worker

A worker entrypoint is an ordinary Python program that names the transports it serves:

```python
# app/worker_high.py
import asyncio

import app.handlers.ingest  # noqa: F401 — importing declares the handler

from app.bus import CONFIG
from message_bus import WorkerFactory

worker = WorkerFactory(CONFIG).worker(["high"])

asyncio.run(worker.run())
```

```sh
uv run python -m app.worker_high
```

What comes back is a `WorkerInterface`, never the broker underneath. Point one process at
`["high"]` and another at `["low"]` and each consumes its own workload.

Most transports are driven by the library's own loop. A broker that brings its own worker —
taskiq does — supplies it instead, and the entrypoint above cannot tell.

### Who retries

The loop makes **exactly one attempt per message** and never retries. Redelivery is something
only a transport can do correctly: it owns the delivery count, the backoff state and the
dead-letter destination. A message that fails is rejected, carrying an `ErrorDetailsStamp`
saying why, and the transport decides what happens next. One undeliverable message does not
stop the worker.

On AMQP that means a retry ladder and real dead-lettering. When attempts run out the message
is republished to `taskiq.dlq` in its original wire format, so it can be inspected and
replayed. A handler can see which attempt it is on:

```python
from message_bus import Envelope, RedeliveryStamp


@as_message_handler(IngestDocument)
async def ingest(message: IngestDocument, envelope: Envelope) -> None:
    attempt = envelope.last(RedeliveryStamp)
```

## Adding behaviour

Every dispatch-side concern is middleware, so adding one changes no existing code:

```python
class RejectOutOfHours:
    async def handle(self, envelope, stack, /):
        if not within_business_hours():
            return envelope          # short-circuit: nothing downstream runs
        return await stack.next().handle(envelope, stack)
```

Two rules carry the producer/consumer split:

- An envelope that arrived **from** a transport carries a `ReceivedStamp` and is never routed
  again, so a consumer cannot re-publish what it consumes.
- Once a sender accepts an envelope the chain **short-circuits**. A message with a transport
  configured is handed off, not also handled locally.

Routing resolves most specific first: a `TransportNamesStamp` on the envelope, then the table
walking the message's bases, then `"*"`, then whatever the message declared. Handler lookup
walks the same hierarchy the same way, so a handler on a marker class still fires for a
subclass that has one of its own.

## Validating messages

Dataclass messages are checked for **shape** by msgspec: a missing or wrongly typed field
raises `MessageDecodingFailedError` at the boundary rather than arriving half-built. Decoding
never coerces — `"3"` is not accepted where an `int` is declared.

A field the message does not declare is **ignored**, which is what lets a producer add one
without redeploying every consumer first. Where both sides ship together and a stray field
means a typo, ask for the other behaviour:

```python
from message_bus import DataclassCodec, JsonSerializer

strict = JsonSerializer(codecs=[DataclassCodec(forbid_unknown_fields=True)])
```

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

> Message annotations are resolved at runtime. If you lint with ruff, set
> `runtime-evaluated-decorators = ["dataclasses.dataclass"]` so field imports are not moved
> into `TYPE_CHECKING` blocks.

## Transports

| Transport | Lives in | Needs | Use for |
| --- | --- | --- | --- |
| `SyncTransport` | `transport/sync/` | — | Handling in-process; local development |
| `InMemoryTransport` | `transport/in_memory/` | — | Tests: records what was dispatched, and can be consumed |
| `TaskiqSender` | `bridge/taskiq/` | `[taskiq]` | Publishing via **any** taskiq broker |
| `TaskiqWorker` | `bridge/taskiq/` | `[taskiq]` | Consuming via taskiq's own worker |
| `create_amqp_broker` | `bridge/amqp/` | `[amqp]` | RabbitMQ, with retries and dead-lettering |

The tree encodes what each piece costs to import:

```
transport/          nothing    — sync, in_memory, the contracts
bridge/taskiq/      [taskiq]   — broker-agnostic: publish, consume, bind handlers
bridge/amqp/        [amqp]     — RabbitMQ only: connection, retry ladder, dead-lettering
```

Everything under `transport/` imports with no extra installed; a transport needing a driver is
a bridge, reachable only once its extra is present. Nothing in `bridge/taskiq/` names a broker
driver either, so `[taskiq]` is usable on its own for Redis, NATS or an in-memory broker. Tests
enforce all three claims.

### Writing your own

Implement two methods and advertise one entry point:

```python
class TransportFactoryInterface(Protocol):
    def supports(self, dsn: Dsn) -> bool: ...
    def create(self, group: Mapping[str, TransportConfig]) -> Mapping[str, SenderInterface]: ...
```

```toml
[project.entry-points."message_bus.transport_factories"]
kafka = "my_package.kafka:KafkaTransportFactory"
```

The entry point **name is the DSN scheme**, which is what keeps discovery lazy: only the module
serving a scheme in use is imported.

Settings reach a factory through its method arguments — each `TransportConfig` carries them —
because settings belong to a transport, not to a factory. One discovered `amqp://` factory
serves two transports pointing at different queues with different retry policies.

A *collaborator* cannot arrive that way. A serializer or a private handlers registry is an
object, not a string, so supplying one means passing the factory yourself:

```python
MessageBusFactory(CONFIG, [AmqpTransportFactory(serializer=mine)]).bus()
```

If your broker owns its own consume loop, also implement `WorkerProvidingInterface`. Most
transports should not: a whole transport is driven by the library's `Worker`.

## Wiring with a container

Every collaborator is a constructor argument and every contract is a `@runtime_checkable`
Protocol, so a DI container can own the whole graph. Nothing here requires one.

Here it is with [wireup](https://github.com/maldoinc/wireup) (2.x). Handlers become objects,
which is what lets them take dependencies:

```python
# app/bus.py
from wireup import injectable

from message_bus import (
    HandlersLocator,
    HandlersLocatorInterface,
    MessageBusConfig,
    MessageBusFactory,
    MessageBusInterface,
    TransportConfig,
)


@injectable
class IngestHandler:
    def __init__(self, documents: DocumentService) -> None:
        self._documents = documents

    async def __call__(self, message: IngestDocument) -> None:
        await self._documents.ingest(message.document_id)


@injectable
def message_bus_config() -> MessageBusConfig:
    return MessageBusConfig(
        transports={"jobs": TransportConfig(AMQP_URL, queue="jobs")},
        routing={IngestDocument: "jobs"},
    )


@injectable
def handlers(ingest: IngestHandler) -> HandlersLocatorInterface:
    registry = HandlersLocator()
    registry.register(IngestDocument, ingest)
    return registry


@injectable
def message_bus(
    config: MessageBusConfig,
    registry: HandlersLocatorInterface,
) -> MessageBusInterface:
    return MessageBusFactory(config, handlers=registry).bus()
```

A route, or anything else, then asks for `MessageBusInterface` and gets a bus it can publish
through. A worker entrypoint resolves one and runs it:

```python
# app/worker.py
import asyncio

import wireup

from app import bus as bus_module
from message_bus import WorkerInterface


async def main() -> None:
    container = wireup.create_async_container(injectables=[bus_module])
    try:
        worker = await container.get(WorkerInterface)
        await worker.run()
    finally:
        await container.close()


asyncio.run(main())
```

<details>
<summary><b>Two things to know</b></summary>

**`@as_message_handler` writes to a process-wide registry.** That is deliberate — declaration
happens at import, as a side effect of defining the function, and needs somewhere to
accumulate. A container cannot reach into it. Under a container, skip the decorator and
`register()` container-built handlers instead, as above; the two approaches do not mix, because
declaring into one registry and resolving from another fails silently.

**A discovered factory is built with no arguments.** So a factory needing a collaborator — a
private registry, your serializer — must be constructed by you and passed in. On AMQP that
matters, because handlers are registered with the broker by the factory:

```python
@injectable
def worker(
    config: MessageBusConfig,
    registry: HandlersLocatorInterface,
) -> WorkerInterface:
    return WorkerFactory(
        config,
        [AmqpTransportFactory(handlers=registry)],   # the factory needs it too
        handlers=registry,
    ).worker(["jobs"])
```

`assert_routes_registered(worker.broker, [IngestDocument])` at startup catches the mistake:
publishing to AMQP does not fail for an unregistered name — the message is accepted and
silently never consumed.

</details>

## Layout

```
message_bus/
├── envelope.py              the message plus its stamps
├── message_registry.py      what @as_message declared: names, default transports
├── message_bus.py           the loop — wrap, then walk the middleware chain
├── worker.py                the other loop — collect, dispatch, ack or reject
├── decorator/               @as_message, @as_message_handler
├── handler/                 which function handles which message
├── middleware/              routing, handling, logging, the chain cursor
├── stamp/                   one class per module
├── transport/
│   ├── sender/              SenderInterface, SendersLocator
│   ├── receiver/            ReceiverInterface, ChainedReceiver
│   ├── sync/  in_memory/    a transport and its factory, each
│   └── serialization/       the wire format, and codec/ for message shapes
└── bridge/
    ├── taskiq/              broker-agnostic publish and consume
    └── amqp/                RabbitMQ on top of it
```

## Development

```sh
uv sync --all-extras
uv run ruff check src tests
uv run ruff format --check src tests
uv run ty check
uv run basedpyright src tests
uv run pytest
```

Two type checkers on purpose — they disagree often enough to be worth both, and `ty` has
already caught a crash `basedpyright` accepted.

## License

[MIT](LICENSE) © xterr
