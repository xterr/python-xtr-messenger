"""A registered handler, and how to call it."""

from __future__ import annotations

import inspect
from dataclasses import dataclass
from functools import cache
from typing import TYPE_CHECKING, TypeAlias, cast, get_type_hints

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
        _hide_container_parameters(handler)
        return cls(
            handler=handler,
            name=name or _name_of(handler),
            wants_envelope=_wants_envelope(handler),
        )

    @classmethod
    def of_type(
        cls,
        handler_type: type,
        handler: Handler,
        name: str | None = None,
    ) -> HandlerDescriptor:
        """Describe ``handler``, taking its shape from ``handler_type``.

        For a handler that does not exist yet — one a container builds when a
        message arrives — so the class is all there is to check. Checking it
        now means a handler with a shape the bus cannot call is rejected
        while wiring rather than on the first message.

        Raises:
            HandlerSignatureError: If the parameters are not a shape the bus
                can call.
        """
        call = _own_call_of(handler_type)
        if call is not None:
            _hide_container_parameters(call)
        if call is None:
            raise HandlerSignatureError(handler_type.__qualname__, ())
        declared = tuple(inspect.signature(call).parameters)[1:]
        return cls(
            handler=handler,
            name=name or handler_type.__qualname__,
            wants_envelope=_decide(declared, _hints_of(call), handler_type.__qualname__),
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


@cache
def _container_hook() -> Callable[[Handler], None] | None:
    """Return the installed container's way of hiding what it fills, if any.

    A dependency-injection container lets a handler declare parameters it
    supplies. Those are not part of the shape the bus calls, so they have to
    be hidden before the signature is read or every such handler would be
    rejected for declaring a parameter the bus cannot pass.

    Detected rather than required, the same way a pydantic codec is: with no
    container installed there are no container parameters to hide, so this
    cannot change behaviour.
    """
    try:
        from wireup.ioc.util import hide_annotated_names  # noqa: PLC0415
    except ImportError:
        return None
    return hide_annotated_names


def _hide_container_parameters(handler: Handler) -> None:
    """Hide the parameters a container fills, so the bus does not see them."""
    hide = _container_hook()
    if hide is not None:
        hide(handler)


def _own_call_of(handler_type: type) -> Handler | None:
    """Return the ``__call__`` ``handler_type`` itself defines, if any.

    Not ``getattr``: every class inherits ``type.__call__`` from its
    metaclass — the thing that makes ``Thing()`` build one — so asking
    whether a class is callable always says yes, and a class that handles
    nothing would be accepted as a handler and fail on the first message.
    """
    for ancestor in handler_type.__mro__:
        found = ancestor.__dict__.get("__call__")
        if found is not None:
            return cast("Handler", found)
    return None


def _hints_of(target: object) -> dict[str, object]:
    try:
        return dict(get_type_hints(target))
    except (NameError, TypeError):
        return {}


def _decide(parameters: tuple[str, ...], hints: dict[str, object], name: str) -> bool:
    """Report whether a handler declaring ``parameters`` wants the envelope.

    The one place the rule lives, so a handler that is a function and one a
    container will build are held to the same shape.

    Raises:
        HandlerSignatureError: If the parameters are not a shape the bus can
            call.
    """
    if len(parameters) < _ENVELOPE_ARITY:
        return False
    if len(parameters) > _ENVELOPE_ARITY or hints.get(parameters[1]) is not Envelope:
        raise HandlerSignatureError(name, parameters)
    return True


def _wants_envelope(handler: Handler) -> bool:
    parameters = tuple(inspect.signature(handler).parameters)
    return _decide(parameters, _hints_of(_annotated(handler)), _name_of(handler))
