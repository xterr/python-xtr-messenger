"""Declaring which function handles which message.

A handler is a transport-independent concept: it consumes a message, and it
neither knows nor cares whether that message arrived in-process or off a
queue. So this decorator lives in the core, and a handler module imports
nothing but the message class::

    @as_message_handler(IngestDocument)
    async def ingest(message: IngestDocument) -> None: ...

Declaring a handler and wiring it to a transport are separate steps. The
declaration populates a registry; a transport picks the registry up later —
:class:`~message_bus.middleware.HandleMessageMiddleware` reads it to invoke
handlers in-process, and
:func:`~message_bus.bridge.taskiq.binding.bind_handlers` registers each entry
as a task at worker startup.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, TypeVar

from message_bus.handler.default_registry import default_registry

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from message_bus.handler.handlers_locator_interface import HandlersLocatorInterface

__all__ = ["as_message_handler"]

HandlerT = TypeVar("HandlerT", bound="Callable[..., Awaitable[None]]")


def as_message_handler(
    message_type: type,
    registry: HandlersLocatorInterface | None = None,
) -> Callable[[HandlerT], HandlerT]:
    """Register the decorated function as a handler for ``message_type``.

    The handler receives the decoded message. Declare a second parameter
    annotated :class:`~message_bus.envelope.Envelope` to also receive the
    envelope, which carries the delivery attempt and any other stamps::

        @as_message_handler(IngestDocument)
        async def ingest(message: IngestDocument, envelope: Envelope) -> None:
            attempt = envelope.last(RedeliveryStamp)
    """
    target = registry if registry is not None else default_registry()

    def decorate(handler: HandlerT) -> HandlerT:
        _ = target.register(message_type, handler)
        return handler

    return decorate
