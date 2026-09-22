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
    return getattr(handler, "__qualname__", repr(handler))


def _wants_envelope(handler: Handler) -> bool:
    parameters = tuple(inspect.signature(handler).parameters)
    if len(parameters) < _ENVELOPE_ARITY:
        return False
    hints = get_type_hints(handler)
    if len(parameters) > _ENVELOPE_ARITY or hints.get(parameters[1]) is not Envelope:
        raise HandlerSignatureError(_name_of(handler), parameters)
    return True
