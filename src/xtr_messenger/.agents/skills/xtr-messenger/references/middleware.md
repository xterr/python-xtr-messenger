# Middleware and worker events

## Writing middleware

```python
from xtr_messenger import Envelope, MiddlewareInterface, StackInterface, as_middleware


@as_middleware("audit")
class Audit(MiddlewareInterface):
    def __init__(self, channel: str = "app") -> None:
        self._channel = channel

    async def handle(self, envelope: Envelope, stack: StackInterface, /) -> Envelope:
        return await stack.next().handle(envelope, stack)
```

Return `envelope` without calling `stack.next()` to short-circuit: nothing downstream runs.

## Naming it in a chain

```python
MessageBusConfig(
    transports={...},
    routing={...},
    middleware=["logging", "audit", {"audit": {"channel": "billing"}}, Audit()],
)
```

An entry may be a registered name, middleware already built, or `{name: {argument: value}}` —
and an entry with arguments is a middleware of its own, beside the bare name. Besides
`@as_middleware`, a name can come from `MessageBusFactory(CONFIG, named={"audit": Audit})`,
which wins over a declared one. An unregistered name raises `UnknownMiddlewareError`; arguments
the constructor does not take raise `InvalidMiddlewareArgumentsError`.

Middleware keeps no per-message state. One instance serves every dispatch, concurrently, and
serves the bus and every worker alike.

## Order

The chain is: holding messages back (`DispatchAfterCurrentBusMiddleware`), then — under a
kernel — `UnitOfWorkMiddleware`, then whatever `middleware` names, then routing
(`SendMessageMiddleware`), then handling (`HandleMessageMiddleware`). `default_middleware=False`
leaves out everything but what `middleware` names.

Two rules carry the producer/consumer split:

- An envelope that arrived from a transport carries a `ReceivedStamp` and is never routed again,
  so a consumer cannot re-publish what it consumes.
- Once a sender accepts an envelope the chain short-circuits; the message is handed off, not
  also handled locally. `sync://` hands the envelope back marked received, which is why it is
  handled here.

## The logging middleware

`"logging"` is the one name the library ships. It writes through the `LoggerInterface` given as
`MessageBusFactory(CONFIG, logger=...)` or `WorkerFactory(CONFIG, logger=...)`; without one it
writes to a null logger. Everything it has to say travels as context, not in the text.

| Level | Shown at | Records |
| --- | --- | --- |
| `NOTICE` | `-v` | One per dispatch: the message type, the transport, the broker's id |
| `INFO` | `-vv` | One per handler that ran, and what it returned |
| `DEBUG` | `-vvv` | One per dispatch, carrying every stamp the envelope came back with |

Nothing is written at normal verbosity. Under a kernel it writes through the `"messenger"`
channel the bundle adds to logging's configuration, and console handlers follow the command's
`-v` flags.

## Worker events

Give a worker an `EventDispatcherInterface`:

```python
from xtr_event_dispatcher import EventDispatcher
from xtr_messenger.event import WorkerMessageFailedEvent

events = EventDispatcher()
events.add_listener(WorkerMessageFailedEvent, lambda e: alert(e.receiver_name, e.error))

worker = WorkerFactory(CONFIG, event_dispatcher=events).worker(["high"])
```

| Event | When | A listener can |
| --- | --- | --- |
| `WorkerStartedEvent` | once, as the worker starts | |
| `WorkerMessageReceivedEvent` | a message was collected | add stamps; `should_handle(value=False)` to skip it |
| `WorkerMessageHandledEvent` | handled, before it is acknowledged | add stamps |
| `WorkerMessageFailedEvent` | handling raised, before it is rejected | read `error`, `will_retry`; add stamps |
| `WorkerRunningEvent` | after each message is settled | `event.worker.stop()` |
| `WorkerStoppedEvent` | once, however the worker stopped | |

A skipped message is **acknowledged**, not rejected. A listener raising on receipt or on success
fails that message like a handler would; one raising on a failure stops the worker. Each message
event names the transport it came from in `receiver_name`. Under taskiq's own worker there is no
`WorkerRunningEvent`. With the event dispatcher bundle active, every worker announces these
through the container's dispatcher with no wiring.
