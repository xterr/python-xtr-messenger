"""A registered handler, and how to call it."""

from __future__ import annotations

import inspect
from dataclasses import dataclass
from functools import cache
from typing import TYPE_CHECKING, TypeAlias, cast, get_type_hints

from xtr_messenger.envelope import Envelope
from xtr_messenger.exception import HandlerSignatureError

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

__all__ = ["Handler", "HandlerDescriptor"]

Handler: TypeAlias = "Callable[..., Awaitable[None]]"

_WITH_ENVELOPE = 2


@dataclass(frozen=True, slots=True)
class HandlerDescriptor:
    """A registered handler, and how to call it.

    ``handler`` is what was declared: a function, a callable object, or a
    class whose instances are callable — built once, when its first message
    arrives, and shared by every message after. ``call`` is what runs. The two
    start out as the same thing, and an integration that has to wrap a handler
    (a container filling its parameters, a span, a timer) replaces ``call``
    while ``handler`` keeps saying what was declared.
    """

    handler: Handler | type
    name: str
    wants_envelope: bool
    call: Handler

    @classmethod
    def of(cls, handler: Handler | type, name: str | None = None) -> HandlerDescriptor:
        """Describe ``handler`` by inspecting the signature it declares.

        The bus calls a handler with the message, and with the envelope too
        when a second parameter is annotated :class:`Envelope`. Parameters a
        dependency-injection container fills — xtr-dependency-injection's
        ``Injected[T]`` — may follow; they are the container's to supply, not
        the bus's.

        Raises:
            HandlerSignatureError: If the parameters are not a shape the bus
                can call.
        """
        label = name or _name_of(handler)
        return cls(
            handler=handler,
            name=label,
            wants_envelope=_wants_envelope(handler, label),
            call=_built_once(handler) if isinstance(handler, type) else handler,
        )

    async def invoke(self, envelope: Envelope) -> None:
        """Call the handler with the message, and the envelope if it asked."""
        if self.wants_envelope:
            await self.call(envelope.message, envelope)
        else:
            await self.call(envelope.message)


def _name_of(handler: Handler | type) -> str:
    named = getattr(handler, "__qualname__", None)
    return named if isinstance(named, str) else type(handler).__qualname__


def _built_once(handler_type: type) -> Handler:
    """Build ``handler_type`` on its first message, and reuse it for every other.

    A handler is a service, not a value: building one per message would cost
    a construction for each of thousands, to throw away what it holds. Lazily,
    so declaring a handler at import time builds nothing.
    """
    built: Handler | None = None

    async def call(*args: object) -> None:
        nonlocal built
        if built is None:
            built = cast("Handler", handler_type())
        await built(*args)

    return call


def _wants_envelope(handler: Handler | type, name: str) -> bool:
    """Report whether ``handler`` takes the envelope after the message.

    Raises:
        HandlerSignatureError: If the parameters are not a shape the bus can
            call.
    """
    parameters, hints = _signature_of(handler, name)
    declared = tuple(parameters)
    own = tuple(p for p in declared if not _supplied_by_container(hints.get(p)))
    # The bus passes its arguments by position, so they have to come first.
    if declared[: len(own)] != own or len(own) > _WITH_ENVELOPE:
        raise HandlerSignatureError(name, declared)
    if len(own) < _WITH_ENVELOPE:
        return False
    if hints.get(own[1]) is not Envelope:
        raise HandlerSignatureError(name, declared)
    return True


def _signature_of(handler: Handler | type, name: str) -> tuple[list[str], dict[str, object]]:
    """Return the parameter names the handler is called with, and their hints.

    Annotations live wherever the code does: on a function, on the
    ``__call__`` of a callable object — an instance has none of its own — or
    on the ``__call__`` a handler class defines, minus ``self``.

    Raises:
        HandlerSignatureError: If ``handler`` is a class that defines no
            ``__call__``.
    """
    if isinstance(handler, type):
        call = _own_call_of(handler)
        if call is None:
            raise HandlerSignatureError(name, ())
        return list(inspect.signature(call).parameters)[1:], _hints_of(call)
    annotated = (
        handler
        if inspect.isfunction(handler) or inspect.ismethod(handler)
        else type(handler).__call__
    )
    return list(inspect.signature(handler).parameters), _hints_of(annotated)


def _own_call_of(handler_type: type) -> Callable[..., object] | None:
    """Return the ``__call__`` ``handler_type`` defines, if any.

    Not ``getattr``: every class inherits ``type.__call__`` from its
    metaclass — the thing that makes ``Thing()`` build one — so asking a class
    for its ``__call__`` always finds something.
    """
    for ancestor in handler_type.__mro__:
        found: object | None = ancestor.__dict__.get("__call__")
        if found is not None:
            return cast("Callable[..., object]", found)
    return None


def _hints_of(target: object) -> dict[str, object]:
    try:
        return dict(get_type_hints(target, include_extras=True))
    except (NameError, TypeError):
        return {}


def _supplied_by_container(hint: object) -> bool:
    """Report whether a container fills a parameter annotated ``hint``.

    Asked of the container package itself — ``Injected[T]``, ``Autowire(...)`` and
    ``Target(...)`` all count — so the bus and the container never disagree.
    Detected rather than required, the same way a pydantic codec is: with no
    container installed there is nothing to recognise, so this cannot change
    behaviour.
    """
    check = _container_check()
    return check is not None and check(hint)


@cache
def _container_check() -> Callable[[object], bool] | None:
    try:
        from xtr_dependency_injection import is_container_supplied  # noqa: PLC0415
    except ImportError:
        return None
    return is_container_supplied
