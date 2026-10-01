---
name: xtr-messenger
description: How to publish and handle messages with xtr-messenger — declaring messages and handlers, routing them to named transports, shaping the middleware chain, running a worker, and testing without a broker. Use when code must send a job, an event or a command to be handled elsewhere, when adding a background worker or a queue, when wiring RabbitMQ/AMQP or taskiq, when writing message handlers, stamps or middleware, when a dispatch raises NoHandlerForMessageError, NoSenderForMessageError, HandlersFailedError or MessageDecodingFailedError, or when adding MessengerBundle to an application on xtr-dependency-injection.
---

# xtr-messenger

Dispatch a message and the bus wraps it in an `Envelope`, walks it through a middleware chain,
and either hands it to a transport or calls its handlers. Where a message goes is configuration:
a routing table maps a message type to one or more named transports, and each transport is a
DSN. The dispatch site never knows which.

## Quick reference

- Declare a message: `@as_message(name="ingest.document.v1")` over a frozen dataclass (or a
  pydantic model with the `pydantic` extra). Always pin an explicit versioned `name`.
- Declare a handler: `@as_message_handler(IngestDocument)` on an async function or a class with
  `async def __call__`.
- Configure once: `MessageBusConfig(transports={...}, routing={...})` with
  `TransportConfig("<dsn>")`. It is inert data and builds nothing.
- Publish: `envelope = await bus.dispatch(message, *stamps)`.
- Build a bus by hand: `MessageBusFactory(CONFIG).bus()`. Build a worker:
  `WorkerFactory(CONFIG).worker(["high"])`, then `await worker.run()`.
- In an application: activate `MessengerBundle`, inject `MessageBusInterface`, and take
  container services in handlers as `Injected[T]`.
- Tests: route to `in-memory://?serialize=true` and inspect `InMemoryTransport.messages`.
- Read a stamp inside a handler by asking for a second parameter annotated `Envelope`.
- Transport DSNs and their settings: [references/transports.md](references/transports.md).

## Declare a message and its handler

```python
from dataclasses import dataclass
from uuid import UUID

from xtr_messenger import Envelope, RedeliveryStamp, as_message, as_message_handler


@as_message(name="ingest.document.v1")
@dataclass(frozen=True, slots=True)
class IngestDocument:
    document_id: UUID
    tenant_id: UUID


@as_message_handler(IngestDocument)
async def ingest(message: IngestDocument, envelope: Envelope) -> None:
    attempt = envelope.last(RedeliveryStamp)  # None on the first delivery
```

- `name` defaults to `module:QualName`, which changes when the class moves. Pin it.
- `@as_message(transport="jobs")` sets a default transport, used only when routing says nothing.
- A consumer decodes only the messages it declared — a name on the wire is never imported. Any
  message crossing a serializer must be declared on the consuming side, or decoding fails with
  `MessageDecodingFailedError`.
- The handler takes the message, and the `Envelope` only when a parameter is annotated
  `Envelope`. Any other shape raises `HandlerSignatureError` at declaration time.
- A handler class is built **once** and shared. Keep per-message state off `self`.
- Every handler of a message runs, in declaration order, each leaving a `HandledStamp` whose
  `result` is what it returned. Lookup walks the message's bases.
- One handler raising does not stop the others; the dispatch then fails with a single
  `HandlersFailedError` carrying each failure in `errors`. A retry reruns **every** handler, so
  keep handlers idempotent.
- Need a registry that is not process-wide: `orders = HandlersLocator()`, then
  `@as_message_handler(PlaceOrder, orders)` and `MessageBusFactory(CONFIG, handlers=orders)`.

## Configure and publish

```python
from xtr_messenger import MessageBusConfig, MessageBusFactory, TransportConfig

CONFIG = MessageBusConfig(
    transports={
        "high": TransportConfig(AMQP_URL, queue="jobs_high"),
        "low": TransportConfig(AMQP_URL, queue="jobs_low"),
        "sync": TransportConfig("sync://"),
    },
    routing={
        UrgentJob: "high",
        AuditRecorded: ["low", "sync"],  # fan out
        "*": "low",  # catch-all
    },
)

bus = MessageBusFactory(CONFIG).bus()
envelope = await bus.dispatch(IngestDocument(document_id=doc_id, tenant_id=tenant_id))
```

A DSN is read when the bus or a worker is built, not when `TransportConfig` is made, so a
missing scheme raises `InvalidDsnError` there. Transports differing only in their query string
share one connection.

Routing resolves most specific first: a `TransportNamesStamp` on the envelope, then the table
walking the message's bases, then `"*"`, then whatever the message declared.

| Setting | Default | Means |
| --- | --- | --- |
| `middleware` | `()` | What runs, in order, ahead of routing and handling |
| `default_middleware` | `True` | `False` drops holding-back, routing and handling entirely |
| `require_sender` | `False` | Refuse a message routed nowhere, with `NoSenderForMessageError` |
| `handle_unrouted` | `False` | Handle a message routed nowhere in this process instead |
| `require_handler` | `True` | Refuse a message handled here that no handler takes |

## Stamps

```python
from dataclasses import dataclass

from xtr_messenger import DispatchAfterCurrentBusStamp, StampInterface, as_stamp


@as_stamp
@dataclass(frozen=True, slots=True)
class TenantStamp(StampInterface):
    tenant_id: str


await bus.dispatch(IngestDocument(doc_id, tenant_id), TenantStamp("acme"))
```

Read them with `envelope.last(TenantStamp)` or `envelope.all(HandledStamp)`; add with
`envelope.with_stamps(...)` and drop with `envelope.without_stamps(TenantStamp)` — an `Envelope`
is immutable. A stamp that must reach the consumer has to be `@as_stamp`-declared, because
decoding is an allow-list; two declared stamps may not share a class name. A
`NonSendableStampInterface` stamp never leaves the process and cannot be declared.

`DispatchAfterCurrentBusStamp` holds a follow-up back until the message being handled has been
handled, every middleware included; if that message fails, the follow-up never goes out.
Failures of held-back messages come back as `DelayedMessageHandlingError`.

`RedispatchMessage(BuildReport(id))` asks for a message to go out again **through routing**;
`RedispatchMessage(BuildReport(id), "urgent")` pins the transport. Nothing needs declaring. It
holds an envelope, which no codec carries, so handle it in the process that creates it.

## Middleware

Every dispatch-side concern is middleware, and which middleware runs is configuration — the bus
and every worker read the same list:

```python
middleware = ["logging", "audit", {"audit": {"channel": "billing"}}, Audit()]
```

Declare one with `@as_middleware("audit")` on a `MiddlewareInterface` subclass whose
`handle(envelope, stack, /)` returns `await stack.next().handle(envelope, stack)`. Middleware
keeps no per-message state: one instance serves every dispatch, on the bus and in every worker.
Full shape, ordering rules, the `"logging"` levels and worker events:
[references/middleware.md](references/middleware.md).

## Run a worker

```python
# app/worker_high.py
import asyncio
import signal

import app.handlers  # noqa: F401 — importing declares the messages and handlers

from app.bus import CONFIG
from xtr_messenger import WorkerFactory


async def main() -> None:
    worker = WorkerFactory(CONFIG).worker(["high"])
    asyncio.get_running_loop().add_signal_handler(signal.SIGTERM, worker.stop)
    await worker.run()


asyncio.run(main())
```

With the `console` extra, `<script> messenger:consume high low` does the same, and
`--time-limit 3600` stops it after an hour. Without a container, hand the command a factory
first: `ConsumeMessagesCommand.use_workers(WorkerFactory(CONFIG))`, imported from
`xtr_messenger.command`.

Every message a worker hands a handler carries a `ReceivedStamp` naming the configured
transport. `stop()` lets the message in hand finish and then returns. Give the worker an
`EventDispatcherInterface` — `WorkerFactory(CONFIG, event_dispatcher=events)` — and it announces
the events in [references/middleware.md](references/middleware.md).

## Testing

Route to `in-memory://` and build the bus and worker from the **same** factory list so they
share the recorder:

```python
from xtr_messenger import (
    InMemoryTransport,
    InMemoryTransportFactory,
    MessageBusConfig,
    MessageBusFactory,
    TransportConfig,
    WorkerFactory,
)

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

await WorkerFactory(CONFIG, factories).worker(["jobs"]).run()  # returns once drained
assert jobs.rejected == ()
```

`?serialize=true` round-trips every message through the serializer, so a field that would only
fail on a real broker fails here. `sent` keeps the whole history of envelopes, `pending` counts
what is undrained, and `clear()` resets between tests.

## Use in an application

1. **Install** — `uv add "xtr-messenger[di,console]"`; add `amqp`, `taskiq` or `pydantic` for
   what you use.
2. **Activate** — `MessengerBundle: {"all": True}` in `BUNDLES` in `<app>/bundles.py`, imported
   from `xtr_messenger.bundle`.
3. **Brings along** — the logging, console and event dispatcher bundles, when those packages are
   installed. Each is optional.
4. **Configure** — required to move anything: with no configuration there are no transports.

   ```python
   # <app>/config/messenger.py
   from xtr_dependency_injection import configure

   from xtr_messenger import MessageBusConfig, TransportConfig

   from app.messages import IngestDocument


   @configure
   def messenger() -> MessageBusConfig:
       return MessageBusConfig(
           transports={"jobs": TransportConfig("amqp://queue")},
           routing={IngestDocument: "jobs"},
       )
   ```

5. **Environment** — nothing required; a broker DSN is usually `env("MESSENGER_DSN")`, read only
   when the bus is built.
6. **Use** — inject `MessageBusInterface` to publish. In a handler, container services arrive
   only through a marker: `Injected[T]`, `Annotated[T, Target("name")]` or
   `Annotated[T, Autowire(param=... | env=...)]`. A bare `T` is **not** injected.

   ```python
   @as_message_handler(IngestDocument)
   async def ingest(message: IngestDocument, db: Injected[Session]) -> None: ...
   ```

   A handler class is a singleton: its constructor takes what lives as long as the handler, and
   per-message dependencies go on `__call__` as `Injected[T]`.
7. **Run** — a worker is `<script> messenger:consume <transport>`.
8. **Check** — `debug:bundles` shows `messenger` as `listed` and `active`; `debug:config
   messenger` shows the resolved transports and routing.
9. **Remove** — drop the `BUNDLES` entry, delete `<app>/config/messenger.py`, then
   `uv remove xtr-messenger` — unless xtr-scheduler is installed, which depends on it.

The bundle registers a `MessageBusInterface`, a `WorkerFactory`, a per-kernel `HandlersLocator`
and the `messenger:consume` command when the console bundle is active. Every handler it finds is
bound at boot, so one asking for something the container cannot provide fails at boot, not on
its first message. Every message is a unit of work: a `lifetime="scoped"` service is built once
per message and released when the message is done with, even when a handler raised.

## Errors

Everything derives from `MessageBusError` and carries typed attributes.

| Error | Raised when |
| --- | --- |
| `HandlerSignatureError` | A handler is declared with a shape the bus cannot call |
| `NoHandlerForMessageError` | A message is to be handled and nothing handles it |
| `NoSenderForMessageError` | A message is routed nowhere and `require_sender=True` |
| `HandlersFailedError` | One or more handlers raised, once every handler has run |
| `DelayedMessageHandlingError` | A held-back message failed after the current one succeeded |
| `InvalidDsnError`, `UnsupportedDsnError` | A DSN has no scheme, or nothing installed serves it |
| `UnknownTransportError` | A route or a worker names a transport that does not exist |
| `UnknownTransportOptionError`, `InvalidTransportOptionError` | A setting is not accepted, or its value is not usable |
| `UnknownMiddlewareError`, `InvalidMiddlewareArgumentsError` | Middleware is named but unregistered, or given arguments it does not take |
| `MessageEncodingFailedError`, `MessageDecodingFailedError`, `UnknownMessageNameError` | A message cannot be put on, or taken off, the wire |
| `NotConsumableError`, `IncompatibleReceiversError`, `MixedDsnError` | A worker is asked to consume something it cannot |

## Do not

- Do not leave `@as_message` without an explicit `name` for anything that outlives a deploy; the
  default changes when the class moves and consumers stop recognising it.
- Do not add parameters to a handler beyond the message and an `Envelope`-annotated one unless
  they are `Injected[...]` — a bare type is not injected and the declaration is refused.
- Do not keep per-message state on a handler instance or on middleware; both are built once.
- Do not expect a retry from the worker loop: it makes exactly one attempt and the transport
  owns redelivery. Make handlers idempotent.
- Do not assume one failing handler stops the rest — every handler runs, then the dispatch fails.
- Do not route a `RedispatchMessage` to a remote transport; it holds an envelope no codec
  carries.
- Do not reach into a transport to call a handler; only the bus calls handlers.
- Do not build the bus and the worker from separate `InMemoryTransportFactory` instances in a
  test — they would record into different queues.
- Do not pass non-string values in `TransportConfig.options`.
