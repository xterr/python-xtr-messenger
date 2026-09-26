"""Declaring which function handles which message.

A handler is a transport-independent concept: it consumes a message, and it
neither knows nor cares whether that message arrived in-process or off a
queue. So this decorator lives in the core, and a handler module imports
nothing but the message class::

    @as_message_handler(IngestDocument)
    async def ingest(message: IngestDocument) -> None: ...

Declaring a handler and wiring it to a transport are separate steps. The
declaration populates a registry, and exactly one thing reads it:
:class:`~xtr_messenger.middleware.HandleMessageMiddleware`, at the end of a
bus. Every transport — ``sync://``, the library's worker, a broker with a
consume loop of its own — hands messages to a bus rather than calling
handlers itself.
"""

from __future__ import annotations

import contextlib
from typing import TYPE_CHECKING, TypeVar, cast

from xtr_messenger.handler.default_registry import default_registry
from xtr_messenger.handler.handlers_registry import HANDLERS_ATTRIBUTE

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from xtr_messenger.handler.handlers_locator_interface import HandlersLocatorInterface

__all__ = ["as_message_handler"]

HandlerT = TypeVar("HandlerT", bound="Callable[..., Awaitable[None]] | type")


def as_message_handler(
    message_type: type,
    registry: HandlersLocatorInterface | None = None,
) -> Callable[[HandlerT], HandlerT]:
    """Register the decorated function as a handler for ``message_type``.

    The handler receives the decoded message. Declare a second parameter
    annotated :class:`~xtr_messenger.envelope.Envelope` to also receive the
    envelope, which carries the delivery attempt and any other stamps::

        @as_message_handler(IngestDocument)
        async def ingest(message: IngestDocument, envelope: Envelope) -> None:
            attempt = envelope.last(RedeliveryStamp)

    A class works too, when its instances are the callable. It is built once
    and shared by every message — by a dependency-injection container when one
    is wired, otherwise with no arguments — so keep per-message state off
    ``self``::

        @as_message_handler(IngestDocument)
        class Ingest:
            async def __call__(self, message: IngestDocument) -> None: ...
    """
    target = registry if registry is not None else default_registry()

    def decorate(handler: HandlerT) -> HandlerT:
        _ = target.register(message_type, handler)
        existing: object = getattr(handler, HANDLERS_ATTRIBUTE, ())
        previous = cast("tuple[type, ...]", existing) if isinstance(existing, tuple) else ()
        with contextlib.suppress(AttributeError, TypeError):
            setattr(handler, HANDLERS_ATTRIBUTE, (*previous, message_type))
        return handler

    return decorate
