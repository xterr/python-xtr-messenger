<div align="center">

# xtr-messenger

**A message bus for Python — envelopes, stamps, a middleware chain, and pluggable transports.**

<img alt="python 3.11+" src="https://img.shields.io/badge/python-%E2%89%A5%203.11-3776AB?logo=python&logoColor=white">
<img alt="core dependencies: 3" src="https://img.shields.io/badge/core%20deps-3-3FB950">
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

- 🪶 **Three core dependencies** — `msgspec`, `typing-extensions` and `xtr-logging`. A broker is an extra.
- 🔌 **Transports are discovered** — one entry point adds a scheme; no fork, no registry to edit.
- 💤 **Lazily loaded** — an app speaking `sync://` never imports a broker library.
- 🎯 **Handlers run in one place** — the same way in-process, in a worker, or under a broker's own loop.
- 🧩 **Protocol-based** — every collaborator is a constructor argument, so a DI container can own the graph.
- 📨 **Dataclasses or pydantic** — both on one bus; dataclasses are checked for shape, models by their own rules.

```python
await bus.dispatch(IngestDocument(document_id=doc.id))
```

Where that goes is configuration. Whether it happens in-process or on RabbitMQ is a DSN.

## Install

```sh
uv add xtr-messenger                    # sync:// and in-memory://
uv add "xtr-messenger[amqp]"            # + RabbitMQ
uv add "xtr-messenger[taskiq]"          # + any other taskiq broker
uv add "xtr-messenger[pydantic]"        # + pydantic messages
uv add "xtr-messenger[wireup]"          # + a pre-wired DI container
uv add "xtr-messenger[console]"         # + the messenger:consume console command
```

| Extra | Brings | For |
| --- | --- | --- |
| *(none)* | `msgspec`, `typing-extensions`, `xtr-logging` | `sync://`, `in-memory://`, the whole core |
| `pydantic` | `pydantic` | Messages validated by a model, not just a shape |
| `taskiq` | `taskiq` | Publishing and consuming over **any** taskiq broker |
| `amqp` | `taskiq-aio-pika` | RabbitMQ, with retries and real dead-lettering |
| `wireup` | `wireup` | Everything pre-wired for a [wireup](https://github.com/maldoinc/wireup) container |
| `console` | `xtr-console` | `messenger:consume`, on an [xtr-console](https://github.com/xterr/python-xtr-console) application |

Requires Python 3.11+. `xtr-logging` is not on PyPI yet; with uv, point it at git:

```toml
[tool.uv.sources]
xtr-logging = { git = "https://github.com/xterr/python-xtr-logging.git" }
```

That one entry is enough — uv reads a git dependency's own `tool.uv.sources`, so the `xtr-clock`
that `xtr-logging` reads the time from is resolved from its repository too.

## Quick start

A message is a frozen dataclass carrying identifiers. `@as_message` declares it, and its
properties live on the class:

```python
from dataclasses import dataclass
from uuid import UUID

from xtr_messenger import as_message


@as_message(name="ingest.document.v1")
@dataclass(frozen=True, slots=True)
class IngestDocument:
    document_id: UUID
    tenant_id: UUID
```

`name` is the contract between producer and consumer, and the only thing they share. It
defaults to `module:QualName`, which changes if the class moves — pin an explicit, versioned
name for anything that outlives a deploy. `@as_message(transport="jobs")` gives the message a
default transport, used when the routing table says nothing about it.

A handler is a function that imports nothing but the message:

```python
from xtr_messenger import as_message_handler


@as_message_handler(IngestDocument)
async def ingest(message: IngestDocument) -> None:
    ...
```

Describe the transports and where messages go, then build a bus:

```python
from xtr_messenger import MessageBusConfig, MessageBusFactory, TransportConfig

CONFIG = MessageBusConfig(
    transports={"sync": TransportConfig("sync://")},
    routing={IngestDocument: "sync"},
)

bus = MessageBusFactory(CONFIG).bus()
await bus.dispatch(IngestDocument(document_id=doc_id, tenant_id=tenant_id))
```

Moving that message onto RabbitMQ later is one line of configuration. The dispatch site does
not change.

## Handlers

The bus calls a handler with the message, and with the envelope too when a second parameter is
annotated `Envelope`. Anything else is refused with `HandlerSignatureError` where the handler is
declared, not on the first message that reaches it.

```python
from xtr_messenger import Envelope, RedeliveryStamp, as_message_handler


@as_message_handler(IngestDocument)
async def ingest(message: IngestDocument, envelope: Envelope) -> None:
    stamp = envelope.last(RedeliveryStamp)          # which delivery attempt this is


@as_message_handler(IngestDocument)
class AuditIngest:                                  # built once, on its first message
    async def __call__(self, message: IngestDocument) -> None: ...
```

A function, a callable object or a class all work. A class is built **once**, on its first
message, and shared by every message after — with no arguments on its own, or by the container
when [one is wired](#wiring-with-a-container). A handler is a service, not a value: building one
per message would cost a construction each, thousands of times a second. Messages may be handled
concurrently, so keep per-message state off `self`.

**Every handler of a message runs**, in the order declared, and each leaves a `HandledStamp`
behind. Lookup walks the message's bases, so a handler on a marker class still fires for a
subclass that has handlers of its own.

Declaring writes to a process-wide registry, which is what lets a handler module import nothing
but the message. Where one registry per process is too coarse — two applications in one test
run, say — declare into a `HandlersLocator` of your own and hand it to the bus or the worker:

```python
from xtr_messenger import HandlersLocator

orders = HandlersLocator()


@as_message_handler(PlaceOrder, orders)
async def place_order(message: PlaceOrder) -> None: ...


bus = MessageBusFactory(CONFIG, handlers=orders).bus()
```

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

A DSN selects the adapter and carries its settings. It is read when the `TransportConfig` is
made, so one without a scheme fails there with `InvalidDsnError` rather than when the bus is
built. Transports differing only in their query string address the same server, so they share
one connection.

| DSN | Transport |
| --- | --- |
| `sync://` | Handles in the calling process, during the dispatch |
| `in-memory://` | Records what was dispatched, for a worker or a test to drain; `?serialize=true` round-trips it |
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

An unrecognised setting is **refused** with `UnknownTransportOptionError`, naming what the scheme
does accept, and a value it cannot use — `prefetch_count=lots` — with
`InvalidTransportOptionError`. A typo in configuration is always a mistake, and one that is
ignored leaves a transport running on defaults nobody chose.

Credentials are not settings — a URL already expresses them, so they stay in the DSN.

</details>

## Running a worker

A worker entrypoint is an ordinary Python program that names the transports it serves:

```python
# app/worker_high.py
import asyncio
import signal

import app.handlers.ingest  # noqa: F401 — importing declares the message and its handler

from app.bus import CONFIG
from xtr_messenger import WorkerFactory


async def main() -> None:
    worker = WorkerFactory(CONFIG).worker(["high"])
    asyncio.get_running_loop().add_signal_handler(signal.SIGTERM, worker.stop)
    await worker.run()


asyncio.run(main())
```

```sh
uv run python -m app.worker_high
```

What comes back is a `WorkerInterface`, never the broker underneath. Point one process at
`["high"]` and another at `["low"]` and each consumes its own workload. `stop()` lets it finish
the message in hand and return — at once, if it is waiting for one.

Most transports are driven by the library's own loop. A broker that brings its own worker —
taskiq does — supplies it instead, and the entrypoint above cannot tell. Either way what it
receives is dispatched into a bus, and the bus calls the handlers.

> On AMQP a worker registers one task per message declared with `@as_message`, so import the
> modules declaring your messages before building it. A message that arrives with no handler
> fails with `NoHandlerForMessageError` and is retried, then dead-lettered — never acknowledged
> and lost.

### From the console

With the `console` extra, `messenger:consume` does the same from an
[xtr-console](https://github.com/xterr/python-xtr-console) application. Importing
`xtr_messenger.command` declares it; it needs only a `WorkerFactory` to build workers with.

```python
# app/console.py
import app.handlers  # noqa: F401

from app.bus import CONFIG
from xtr_console import Application
from xtr_messenger import WorkerFactory
from xtr_messenger.command import ConsumeMessagesCommand

ConsumeMessagesCommand.use_workers(WorkerFactory(CONFIG))
raise SystemExit(Application("app").run())
```

```sh
uv run python -m app.console messenger:consume high low
uv run python -m app.console messenger:consume high --time-limit 3600
```

SIGTERM, or the time limit running out, stops the worker once the message in hand is settled;
Ctrl-C cancels it. With a [container](#wiring-with-a-container), skip `use_workers()`: import
`xtr_messenger.command` before the console's `injectables()`, and the command is built from the
`WorkerFactory` the messenger's `injectables()` provide — handlers wired to the container.

```python
import xtr_messenger.command  # noqa: F401
from xtr_console.integration import wireup as console

container = wireup.create_async_container(
    injectables=[services, *messenger.injectables(CONFIG), *console.injectables(Application("app"))],
)
raise SystemExit(await (await container.get(Application)).run_async())
```

### Who retries

The loop makes **exactly one attempt per message** and never retries. Redelivery is something
only a transport can do correctly: it owns the delivery count, the backoff state and the
dead-letter destination. A message that fails is rejected, carrying an `ErrorDetailsStamp`
saying why, and the transport decides what happens next. One undeliverable message does not
stop the worker.

On AMQP that means a retry ladder and real dead-lettering. When attempts run out the message
is republished to `taskiq.dlq` — or the `dead_letter_queue` you name — in its original wire
format, so it can be inspected and replayed. A handler that asks for the envelope sees which
attempt it is on in its `RedeliveryStamp`.

## Shaping the bus

Every dispatch-side concern is middleware, so adding one changes no existing code:

```python
from xtr_messenger import Envelope, LoggingMiddleware, MiddlewareInterface, StackInterface


class RejectOutOfHours(MiddlewareInterface):
    async def handle(self, envelope: Envelope, stack: StackInterface, /) -> Envelope:
        if not within_business_hours():
            return envelope          # short-circuit: nothing downstream runs
        return await stack.next().handle(envelope, stack)


bus = MessageBusFactory(CONFIG).bus([LoggingMiddleware(logger), RejectOutOfHours()])
```

`LoggingMiddleware` reports each dispatch through an
[xtr-logging](https://github.com/xterr/python-xtr-logging) `LoggerInterface`, with everything it
has to say travelling as context rather than baked into the text:

```python
from xtr_logging import ConsoleHandler, Logger

logger = Logger("messenger", [ConsoleHandler()])
# [2026-09-24T12:30:45+03:00] messenger.NOTICE: message dispatched
#   {"message_type":"IngestDocument","transport":"high","message_id":"id-1"} []
```

**What it says is graded by severity, so a command's `-v` flags decide how much of a dispatch
it shows.** A `ConsoleHandler` prints notices at `-v`, info at `-vv` and everything at `-vvv`:

| Level | Shown at | Records |
| --- | --- | --- |
| `NOTICE` | `-v` | One per dispatch: the message type, the transport it went to, the id the broker gave it |
| `INFO` | `-vv` | One per handler that ran, and what it returned |
| `DEBUG` | `-vvv` | One per dispatch, carrying every stamp the envelope came back with |

Nothing appears at normal verbosity — a dispatch is routine, and a bus running thousands a
second should not say so unasked. Each tier adds what the one above it left out rather than
repeating it, so `-vvv` on a worker reads as a trace and a plain run stays silent.

Given no logger it writes to a `NullLogger`, so the middleware costs nothing until an
application hands it one.

**With a container you add nothing at all.** A container that provides a `LoggerInterface` —
xtr-logging's own
[wireup integration](https://github.com/xterr/python-xtr-logging#wiring-with-a-container) does —
has every dispatch logged, on the bus and in every worker, without being asked:

```python
container = wireup.create_async_container(
    injectables=[services, *logging.injectables(LOGGING), *messenger.injectables(CONFIG)],
)
```

That is the whole of it. Under [xtr-console](https://github.com/xterr/python-xtr-console) the
same container makes its console handlers follow every command it runs, so the `-v` flags do the
rest and `messenger:consume -vv` reads out every handler that ran. Provide no logger and nothing
is added.

Your middleware runs first, in the order given. Routing and handling always come last, in that
order, and two rules carry the producer/consumer split:

- An envelope that arrived **from** a transport carries a `ReceivedStamp` and is never routed
  again, so a consumer cannot re-publish what it consumes.
- Once a sender accepts an envelope the chain **short-circuits**. A message with a transport
  configured is handed off, not also handled locally — unless the sender hands it back
  received, which is all `sync://` does, and then the bus handles it here.

Handlers are called in exactly one place, at the end of that chain. No transport calls one
itself, so what runs is the same whether a message is handled in-process, by the library's
worker, or by a broker's own; and a message routed to be handled with nothing to handle it
fails loudly everywhere, with `NoHandlerForMessageError`.

A message routed nowhere passes quietly by default. `bus(require_sender=True)` refuses it with
`NoSenderForMessageError`; `bus(handle_unrouted=True)` handles it in-process instead.

Routing resolves most specific first: a `TransportNamesStamp` on the envelope, then the table
walking the message's bases, then `"*"`, then whatever the message declared.

## Validating messages

Dataclass messages are checked for **shape** by msgspec: a missing or wrongly typed field
raises `MessageDecodingFailedError` at the boundary rather than arriving half-built. Decoding
never coerces — `"3"` is not accepted where an `int` is declared.

A field the message does not declare is **ignored**, which is what lets a producer add one
without redeploying every consumer first. Where both sides ship together and a stray field
means a typo, ask for the other behaviour:

```python
from xtr_messenger import DataclassCodec, JsonSerializer

strict = JsonSerializer(codecs=[DataclassCodec(forbid_unknown_fields=True)])
```

For rules a type cannot express, model the message with pydantic. Install the extra and it is
picked up automatically; both styles work on the same bus. Strictness is the model's own
choice, so a validator written to normalise its input keeps working.

```python
from decimal import Decimal
from typing import Annotated, ClassVar
from uuid import UUID

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

### Errors

Everything the library raises derives from `MessageBusError`, and carries what went wrong as
typed attributes rather than only a message.

| Error | Raised when |
| --- | --- |
| `HandlerSignatureError` | A handler is declared with a shape the bus cannot call |
| `InvalidDsnError` | A DSN has no scheme |
| `UnknownTransportOptionError`, `InvalidTransportOptionError` | A setting is not accepted, or its value is not usable |
| `UnsupportedDsnError` | No installed transport serves a scheme |
| `UnknownTransportError` | A route or a worker names a transport the configuration does not define |
| `MixedDsnError` | One AMQP worker is asked to serve two servers |
| `NotConsumableError` | A worker is asked to consume a transport that can only send |
| `NoSenderForMessageError` | A message is routed nowhere and the bus requires a sender |
| `NoHandlerForMessageError` | A message is to be handled and nothing handles it |
| `UnregisteredHandlerError` | A handler class is declared after the wireup container was built |
| `MessageEncodingFailedError` | A message cannot be put on the wire |
| `MessageDecodingFailedError`, `UnknownMessageNameError` | A payload cannot be turned back into its message |

## Testing your application

`in-memory://` records instead of sending, and can be drained by a worker, so a test runs the
whole path without a broker. Build the bus and the worker from the same factory instance so
they share the recorder:

```python
from xtr_messenger import InMemoryTransport, InMemoryTransportFactory, WorkerFactory

CONFIG = MessageBusConfig(
    transports={"jobs": TransportConfig("in-memory://?serialize=true")},
    routing={IngestDocument: "jobs"},
)
factories = [InMemoryTransportFactory()]

bus = MessageBusFactory(CONFIG, factories).bus()
_ = await bus.dispatch(IngestDocument(document_id=doc_id, tenant_id=tenant_id))

jobs = factories[0].create(CONFIG.transports)["jobs"]
assert isinstance(jobs, InMemoryTransport)
assert jobs.messages == (IngestDocument(document_id=doc_id, tenant_id=tenant_id),)

await WorkerFactory(CONFIG, factories).worker(["jobs"]).run()   # returns once drained
assert jobs.rejected == ()
```

`?serialize=true` round-trips every message through the serializer on the way in, so a field
that would only fail on a real broker fails in the test instead. `sent` keeps the whole history
while a worker drains the queue, and `clear()` resets it between tests.

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
bridge/taskiq/      [taskiq]   — broker-agnostic: publish, consume, bind a bus
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
[project.entry-points."xtr_messenger.transport_factories"]
kafka = "my_package.kafka:KafkaTransportFactory"
```

The entry point **name is the DSN scheme**, which is what keeps discovery lazy: only the module
serving a scheme in use is imported. A sender that can also be consumed implements
`TransportInterface` — `send`, plus `get`, `ack` and `reject` — and the library's `Worker`
drives it.

Settings reach a factory through its method arguments — each `TransportConfig` carries them —
because settings belong to a transport, not to a factory. One discovered `amqp://` factory
serves two transports pointing at different queues with different retry policies.

A *collaborator* cannot arrive that way. A serializer is an object, not a string, so supplying
one means passing the factory yourself:

```python
MessageBusFactory(CONFIG, [AmqpTransportFactory(serializer=mine)]).bus()
```

Handlers never reach a transport at all. Only the bus calls them, and every transport hands
messages to a bus instead — `sync://` hands the envelope straight back marked received, and a
worker dispatches what it receives. So a private registry given to the bus or the worker
reaches every transport from there.

If your broker owns its own consume loop, also implement `WorkerProvidingInterface`, and dispatch
what it receives into the `bus` its `worker()` is given. Most transports should not: a whole
transport is driven by the library's `Worker`.

## Wiring with a container

Handlers usually need things — a database session, a client, a unit of work. Every collaborator
here is a constructor argument and every contract is a `@runtime_checkable` Protocol, so a
container can own the whole graph. Nothing requires one.

With the `wireup` extra, a handler asks for what it needs the way wireup always does:

```python
# app/handlers.py
from wireup import Injected

from xtr_messenger import as_message_handler


@as_message_handler(IngestDocument)
async def ingest(message: IngestDocument, db: Injected[Session]) -> None:
    await db.record(message.document_id)


@as_message_handler(IssueInvoice)
class IssueInvoiceHandler:
    def __init__(self, invoices: InvoiceRepository) -> None:          # once
        self._invoices = invoices

    async def __call__(self, message: IssueInvoice, db: Injected[Session]) -> None:  # per message
        await self._invoices.issue(db, message.invoice_id)
```

Nothing from this library appears there beyond the decorator. Parameters marked `Injected[T]` —
or `Annotated[T, Inject(config=...)]`, `Inject(qualifier=...)` — are recognised as the
container's to fill, so the bus still calls the handler with just the message (and the envelope,
if asked for). They must come after those.

A handler *class* needs no `@injectable`: the integration registers it with the container as a
**singleton**. Its constructor is resolved once, so it takes what lives as long as the handler —
a repository, a client. Whatever a single message needs goes on `__call__`, filled on every call.
Ask the constructor for something scoped and the container refuses to build, naming the
parameter; that dependency belongs on `__call__`.

Then one call, where the container is built:

```python
# app/worker.py
import asyncio

import wireup

import app.handlers  # noqa: F401 — importing declares the handlers
from app import services
from xtr_messenger import WorkerInterface
from xtr_messenger.integration import wireup as messenger


async def main() -> None:
    container = wireup.create_async_container(
        injectables=[services, *messenger.injectables(CONFIG, transports=["jobs"])],
    )
    try:
        await (await container.get(WorkerInterface)).run()
    finally:
        await container.close()


asyncio.run(main())
```

The container now provides a `MessageBusInterface`, a `WorkerInterface` (only when `transports`
names some — a process that only publishes leaves it out), a `WorkerFactory` for workers whose
transports are chosen later, and the `MessageBusConfig`. The bus
and the worker share their transports, so a handler publishing from inside the worker reuses its
connection. Any service takes the bus like any other dependency:

```python
@injectable
class OrderService:
    def __init__(self, bus: MessageBusInterface) -> None:
        self._bus = bus
```

Handler classes are registered as the container is built, so import the modules declaring them
before calling `injectables()`; one declared afterwards is refused with
`UnregisteredHandlerError`. Resolving the bus or the worker wires the handlers to the container,
and a handler asking for something the container cannot provide is refused then, not on its first
message. Wiring another container — one per test, say — rebinds them, and
`container.override(...)` reaches handlers like anything else.

**A scoped dependency is one per handler call.** A call asking for one gets a scope of its own,
so a `lifetime="scoped"` session is built when the message arrives and released when the handler
finishes, including on failure. A call asking for nothing scoped opens no scope at all.

<details>
<summary><b>Configuration from the container</b></summary>

Leave `config` out and provide a `MessageBusConfig` yourself, when it is read from settings:

```python
@injectable
def bus_config(dsn: Annotated[str, Inject(config="amqp_url")]) -> MessageBusConfig:
    return MessageBusConfig(transports={"jobs": TransportConfig(dsn)}, routing={"*": "jobs"})


container = wireup.create_async_container(
    injectables=[services, bus_config, *messenger.injectables(transports=["jobs"])],
    config={"amqp_url": os.environ["AMQP_URL"]},
)
```

</details>

## Layout

```
xtr_messenger/
├── envelope.py              the message plus its stamps
├── message_registry.py      what @as_message declared: names, default transports
├── message_bus_config.py    which transports exist, and where messages go
├── message_bus_factory.py   builds the bus a process publishes through
├── xtr_messenger.py           the loop — wrap, then walk the middleware chain
├── worker_factory.py        builds what a worker process runs
├── worker.py                the other loop — collect, dispatch, ack or reject
├── dsn.py                   reading a transport's DSN
├── decorator/               @as_message, @as_message_handler
├── handler/                 which function handles which message, and how to call it
├── middleware/              routing, handling, logging, the chain cursor
├── stamp/                   one class per module
├── exception/               one error per module, all a MessageBusError
├── transport/
│   ├── sender/              SenderInterface, SendersLocator — the routing table
│   ├── receiver/            ReceiverInterface, ChainedReceiver
│   ├── sync/  in_memory/    a transport and its factory, each
│   └── serialization/       the wire format, and codec/ for message shapes
├── bridge/
│   ├── taskiq/              broker-agnostic publish and consume
│   └── amqp/                RabbitMQ on top of it
├── command/
│   └── consume.py           messenger:consume, on xtr-console
└── integration/
    └── wireup.py            everything pre-wired for a wireup container
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
already caught a crash `basedpyright` accepted. Both run strict on the tests too, with nothing
suppressed.

The suite mirrors the source tree. `tests/unit/` holds a `test_<module>.py` for each module,
testing it alone against small fakes; `tests/integration/` holds what needs several real
components or a fresh interpreter — end-to-end flows, and the checks that a module never
imports a library it should not. Shared message classes and fakes live in `tests/support/`.

## License

[MIT](LICENSE) © xterr
