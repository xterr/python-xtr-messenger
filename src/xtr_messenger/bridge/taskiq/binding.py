"""Wiring a bus onto a taskiq broker.

taskiq looks a message up by task name, so every declared message becomes a
task under its own name — the same name
:class:`~xtr_messenger.bridge.taskiq.taskiq_sender.TaskiqSender` publishes to.
Each task only rebuilds the envelope and dispatches it into a bus. Which
handlers run is the bus's business, decided in one place whether a message
arrived through taskiq, through the library's own worker, or through
``sync://``::

    import app.handlers.ingest  # noqa: F401 — declares the message and its handler

    bind_bus(broker, MessageBus([HandleMessageMiddleware()]))
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import msgspec
from taskiq import Context, TaskiqDepends

from xtr_messenger.exception import MessageDecodingFailedError
from xtr_messenger.message_registry import declared_names
from xtr_messenger.stamp import ReceivedStamp, RedeliveryStamp
from xtr_messenger.transport.serialization import EncodedEnvelope, JsonSerializer

from .labels import HEADERS_LABEL, RETRIES_LABEL

if TYPE_CHECKING:
    from collections.abc import Callable, Coroutine

    from taskiq import AsyncBroker, AsyncTaskiqDecoratedTask

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


def bind_bus(
    broker: AsyncBroker,
    bus: MessageBusInterface,
    serializer: SerializerInterface | None = None,
) -> tuple[str, ...]:
    """Register a task per declared message on ``broker``, dispatching into ``bus``.

    Call this once at worker startup, after importing the modules that
    declare messages with :func:`~xtr_messenger.decorator.as_message`. Binding
    again is harmless — each task replaces the one under the same name.

    Returns:
        The task names registered.
    """
    wire = serializer if serializer is not None else JsonSerializer()
    names = declared_names()
    for task_name in names:
        _registered: AsyncTaskiqDecoratedTask[..., Coroutine[None, None, None]] = (
            broker.register_task(_task_for(bus, task_name, wire), task_name=task_name)
        )
    return names


def _task_for(
    bus: MessageBusInterface,
    task_name: str,
    serializer: SerializerInterface,
) -> Callable[..., Coroutine[None, None, None]]:
    async def run(body: str, context: Context = _CONTEXT) -> None:
        envelope = _rebuild(body, context, task_name, serializer)
        _ = await bus.dispatch(envelope.message, *envelope.stamps)

    run.__name__ = task_name.rpartition(".")[2]
    run.__qualname__ = task_name
    return run


def _rebuild(
    body: str, context: Context, task_name: str, serializer: SerializerInterface
) -> Envelope:
    labels = context.message.labels
    headers = _headers_from(labels.get(HEADERS_LABEL), task_name)
    envelope = serializer.decode(EncodedEnvelope(body=body, headers=headers))
    return envelope.with_stamps(
        ReceivedStamp("taskiq"),
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
