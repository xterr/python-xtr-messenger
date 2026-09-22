"""A registered handler, and how to call it."""

from __future__ import annotations

import inspect
from dataclasses import dataclass
from typing import TYPE_CHECKING, TypeAlias, get_type_hints

from message_bus.envelope import Envelope
from message_bus.exception import HandlerSignatureError

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

__all__ = ["Handler", "HandlerDescriptor"]

Handler: TypeAlias = "Callable[..., Awaitable[None]]"

_ENVELOPE_ARITY = 2


@dataclass(frozen=True, slots=True)
class HandlerDescriptor:
    """A registered handler, and how to call it."""

    handler: Handler
    name: str
    wants_envelope: bool

    @classmethod
    def of(cls, handler: Handler, name: str | None = None) -> HandlerDescriptor:
        """Describe ``handler`` by inspecting the signature it declares.

        Raises:
            HandlerSignatureError: If the parameters are not a shape the bus
                can call.
        """
        return cls(
            handler=handler,
            name=name or _name_of(handler),
            wants_envelope=_wants_envelope(handler),
        )

    async def invoke(self, envelope: Envelope) -> None:
        """Call the handler with the message, and the envelope if it asked."""
        if self.wants_envelope:
            await self.handler(envelope.message, envelope)
        else:
            await self.handler(envelope.message)


def _name_of(handler: Handler) -> str:
    named = getattr(handler, "__qualname__", None)
    return named if isinstance(named, str) else type(handler).__qualname__


def _annotated(handler: Handler) -> object:
    """Return the object carrying ``handler``'s annotations.

    A handler need not be a function. A dependency-injection container builds
    objects, so a handler is often an instance whose ``__call__`` does the
    work — and annotations live on that method, not on the instance, where
    :func:`typing.get_type_hints` would find nothing and every such handler
    would be rejected for not annotating a parameter it had annotated.
    """
    if inspect.isfunction(handler) or inspect.ismethod(handler):
        return handler
    call = getattr(type(handler), "__call__", None)  # noqa: B004
    return call if call is not None else handler


def _wants_envelope(handler: Handler) -> bool:
    parameters = tuple(inspect.signature(handler).parameters)
    if len(parameters) < _ENVELOPE_ARITY:
        return False
    try:
        hints = get_type_hints(_annotated(handler))
    except (NameError, TypeError):
        hints = {}
    if len(parameters) > _ENVELOPE_ARITY or hints.get(parameters[1]) is not Envelope:
        raise HandlerSignatureError(_name_of(handler), parameters)
    return True
