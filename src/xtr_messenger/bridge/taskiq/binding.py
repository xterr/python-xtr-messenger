"""Wiring a bus onto a taskiq broker.

taskiq looks a message up by task name, so every declared message becomes a
task under its own name — the same name
:class:`~xtr_messenger.bridge.taskiq.taskiq_sender.TaskiqSender` publishes to.
Each task only rebuilds the envelope and dispatches it into a bus. Which
handlers run is the bus's business, decided in one place whether a message
arrived through taskiq, through the library's own worker, or through
``sync://``::

    import app.handlers.ingest  # noqa: F401 — declares the message and its handler

    bind_bus(broker, MessageBusFactory(CONFIG, logger=logger).bus())

A bus from the factory runs the middleware the configuration names; what it
dispatches arrives received, so it is handled rather than routed again.

Given an event dispatcher, each task announces its message the way the
library's own worker does — received, then handled or failed — so listeners
see one sequence whichever worker runs. A message a listener skips returns
normally, which taskiq acknowledges; a failure is announced, then raised for
taskiq's retry and dead-letter middleware to act on.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, final

import msgspec
from taskiq import Context, TaskiqDepends

from xtr_messenger._handling import Failed, announce_failure, handle
from xtr_messenger.exception import MessageDecodingFailedError
from xtr_messenger.message_registry import declared_names
from xtr_messenger.stamp import ReceivedStamp, RedeliveryStamp
from xtr_messenger.transport.serialization import EncodedEnvelope, JsonSerializer

from .labels import HEADERS_LABEL, QUEUE_LABEL, RETRIES_LABEL

if TYPE_CHECKING:
    from collections.abc import Callable, Coroutine, Mapping

    from taskiq import AsyncBroker, AsyncTaskiqDecoratedTask
    from xtr_event_dispatcher_contracts import EventDispatcherInterface

    from xtr_messenger.envelope import Envelope
    from xtr_messenger.message_bus_interface import MessageBusInterface
    from xtr_messenger.transport.serialization import SerializerInterface

__all__ = ["bind_bus"]

#: Reported when the ``_retries`` label is absent or unreadable. The sender
#: always sets it, so its absence is anomalous — treating it as a very late
#: attempt keeps a handler's "is this the final try?" check on the safe side.
_LOST_LABEL_ATTEMPT = 1_000_000

#: taskiq requires its dependency marker as a parameter default. One shared
#: marker avoids a call in the default expression; taskiq replaces it with
#: the real Context before the task body runs.
_CONTEXT: Context = TaskiqDepends()

#: What a delivery is reported as received from when no transport name fits it.
_UNNAMED = "taskiq"


@final
class _Binding:
    """What every task bound by one :func:`bind_bus` call shares."""

    __slots__ = ("bus", "dispatcher", "max_attempts", "receiver_names", "serializer")

    def __init__(
        self,
        *,
        bus: MessageBusInterface,
        serializer: SerializerInterface,
        dispatcher: EventDispatcherInterface | None,
        receiver_names: Mapping[str | None, str],
        max_attempts: int | None,
    ) -> None:
        self.bus = bus
        self.serializer = serializer
        self.dispatcher = dispatcher
        self.receiver_names = receiver_names
        self.max_attempts = max_attempts

    def receiver_name(self, labels: Mapping[str, object]) -> str:
        """Name the transport a delivery came from, by the queue it was published to.

        A single transport needs no lookup. Otherwise the ``queue_name`` label
        says which queue — absent for the broker's default queue, which is
        the transport naming no queue.
        """
        names = self.receiver_names
        if len(names) == 1:
            return next(iter(names.values()))
        queue = labels.get(QUEUE_LABEL)
        return names.get(queue if isinstance(queue, str) else None, _UNNAMED)

    def will_retry(self, envelope: Envelope) -> bool:
        """Tell whether taskiq delivers ``envelope`` again after this failure."""
        if self.max_attempts is None:
            return False
        stamp = envelope.last(RedeliveryStamp)
        attempt = stamp.retry_count if stamp is not None else _LOST_LABEL_ATTEMPT
        return attempt < self.max_attempts - 1


def bind_bus(  # noqa: PLR0913 — everything past `serializer` is keyword-only
    broker: AsyncBroker,
    bus: MessageBusInterface,
    serializer: SerializerInterface | None = None,
    *,
    event_dispatcher: EventDispatcherInterface | None = None,
    receiver_names: Mapping[str | None, str] | None = None,
    max_attempts: int | None = None,
) -> tuple[str, ...]:
    """Register a task per declared message on ``broker``, dispatching into ``bus``.

    Call this once at worker startup, after importing the modules that
    declare messages with :func:`~xtr_messenger.decorator.as_message`. Binding
    again is harmless — each task replaces the one under the same name.

    Args:
        broker: The broker to register the tasks on.
        bus: What each task dispatches its message into.
        serializer: How messages were encoded; JSON when omitted.
        event_dispatcher: Hears the worker events of
            :mod:`xtr_messenger.event` for every message.
        receiver_names: The transport name for each queue the broker
            consumes, ``None`` standing for its default queue. It is what a
            message is reported as received from; without it, ``"taskiq"``.
        max_attempts: How many deliveries the broker makes in all, which
            decides whether a failure is announced as one that will be
            retried. Unknown when omitted, and then never reported as such.

    Returns:
        The task names registered.
    """
    binding = _Binding(
        bus=bus,
        serializer=serializer if serializer is not None else JsonSerializer(),
        dispatcher=event_dispatcher,
        receiver_names=receiver_names if receiver_names is not None else {},
        max_attempts=max_attempts,
    )
    names = declared_names()
    for task_name in names:
        _registered: AsyncTaskiqDecoratedTask[..., Coroutine[None, None, None]] = (
            broker.register_task(_task_for(binding, task_name), task_name=task_name)
        )
    return names


def _task_for(binding: _Binding, task_name: str) -> Callable[..., Coroutine[None, None, None]]:
    async def run(body: str, context: Context = _CONTEXT) -> None:
        labels = context.message.labels
        name = binding.receiver_name(labels)
        envelope = _rebuild(body, labels, task_name, binding.serializer, name)
        outcome = await handle(binding.bus, envelope, name, binding.dispatcher)
        if isinstance(outcome, Failed):
            await announce_failure(
                outcome,
                name,
                binding.dispatcher,
                _settled_by_raising,
                will_retry=binding.will_retry(outcome.envelope),
            )
            raise outcome.error

    run.__name__ = task_name.rpartition(".")[2]
    run.__qualname__ = task_name
    return run


async def _settled_by_raising(envelope: Envelope) -> None:
    """Leave settling to taskiq, which acts on the exception the task raises."""
    del envelope


def _rebuild(
    body: str,
    labels: Mapping[str, object],
    task_name: str,
    serializer: SerializerInterface,
    receiver_name: str,
) -> Envelope:
    headers = _headers_from(labels.get(HEADERS_LABEL), task_name)
    envelope = serializer.decode(EncodedEnvelope(body=body, headers=headers))
    return envelope.with_stamps(
        ReceivedStamp(receiver_name),
        RedeliveryStamp(_attempt_from(labels.get(RETRIES_LABEL))),
    )


def _headers_from(raw: object, task_name: str) -> dict[str, str]:
    if not isinstance(raw, str):
        return {"type": task_name}
    try:
        return msgspec.json.decode(raw, type=dict[str, str])
    except msgspec.DecodeError as exc:
        raise MessageDecodingFailedError(
            f"headers label must hold a JSON object of strings: {exc}", task_name
        ) from exc


def _attempt_from(raw: object) -> int:
    if isinstance(raw, bool) or not isinstance(raw, (int, str)):
        return _LOST_LABEL_ATTEMPT
    try:
        return int(raw)
    except ValueError:
        return _LOST_LABEL_ATTEMPT
