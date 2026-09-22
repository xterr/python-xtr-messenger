"""Wiring declared handlers onto a taskiq broker.

Handlers are declared with :func:`~message_bus.decorator.as_message_handler`,
which knows nothing about transports. This module performs the other half:
at worker startup it walks a registry and registers each handler as a task,
under the message's own name — the same name
:class:`~message_bus.bridge.taskiq.taskiq_sender.TaskiqSender` publishes to.

Handler modules therefore never import a broker. They are imported for their
registration side effect, and the broker is named once, here::

    import app.handlers.ingest  # noqa: F401 — registers the handler

    bind_handlers(broker)
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import msgspec
from taskiq import Context, TaskiqDepends

from message_bus.exception import MessageDecodingFailedError
from message_bus.handler import default_registry
from message_bus.message_registry import name_of
from message_bus.stamp import ReceivedStamp, RedeliveryStamp
from message_bus.transport.serialization import EncodedEnvelope, JsonSerializer

from .labels import HEADERS_LABEL, RETRIES_LABEL

if TYPE_CHECKING:
    from collections.abc import Callable, Coroutine

    from taskiq import AsyncBroker, AsyncTaskiqDecoratedTask

    from message_bus.envelope import Envelope
    from message_bus.handler import HandlerDescriptor, HandlersLocatorInterface
    from message_bus.transport.serialization import SerializerInterface

__all__ = ["bind_handlers"]

#: Reported when the ``_retries`` label is absent or unreadable. The sender
#: always sets it, so its absence is anomalous — treating it as a very late
#: attempt keeps a handler's "is this the final try?" check on the safe side.
_LOST_LABEL_ATTEMPT = 1_000_000

#: taskiq requires its dependency marker as a parameter default. One shared
#: marker avoids a call in the default expression; taskiq replaces it with
#: the real Context before the task body runs.
_CONTEXT: Context = TaskiqDepends()


def bind_handlers(
    broker: AsyncBroker,
    registry: HandlersLocatorInterface | None = None,
    serializer: SerializerInterface | None = None,
) -> tuple[str, ...]:
    """Register every declared handler on ``broker``, returning the task names.

    Call this once at worker startup, after importing the modules that
    declare handlers. Registering the same handler twice is harmless — the
    second registration replaces the first under the same name.
    """
    source = registry if registry is not None else default_registry()
    wire = serializer if serializer is not None else JsonSerializer()
    names: list[str] = []
    for message_type, descriptor in source.bindings():
        task_name = name_of(message_type)
        task = _task_for(descriptor, task_name, wire)
        _registered: AsyncTaskiqDecoratedTask[..., Coroutine[None, None, None]] = (
            broker.register_task(task, task_name=task_name)
        )
        names.append(task_name)
    return tuple(names)


def _task_for(
    descriptor: HandlerDescriptor,
    task_name: str,
    serializer: SerializerInterface,
) -> Callable[..., Coroutine[None, None, None]]:
    async def run(body: str, context: Context = _CONTEXT) -> None:
        await descriptor.invoke(_rebuild(body, context, task_name, serializer))

    # Named from the descriptor, not the handler: a handler built by a
    # dependency-injection container is an object, and an object has no
    # __name__ — reading one off it crashed at worker startup.
    run.__name__ = descriptor.name.rpartition(".")[2]
    run.__qualname__ = descriptor.name
    run.__doc__ = getattr(descriptor.handler, "__doc__", None)
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
