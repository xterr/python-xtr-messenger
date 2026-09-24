"""The registry binding message types to their handlers."""

from __future__ import annotations

from typing import TYPE_CHECKING, final

from typing_extensions import override

from .handler_descriptor import HandlerDescriptor
from .handlers_locator_interface import HandlersLocatorInterface

if TYPE_CHECKING:
    from collections.abc import Callable

    from .handler_descriptor import Handler

__all__ = ["HandlersLocator"]


@final
class HandlersLocator(HandlersLocatorInterface):
    """Binds message types to the handlers that consume them.

    Lookup walks the message's method resolution order, so registering a
    base class or a shared marker class handles every subclass — alongside
    whatever the subclass registers of its own.
    """

    __slots__ = ("_declared", "_decorators", "_handlers")

    def __init__(self) -> None:
        """Start empty."""
        self._declared: dict[type, tuple[HandlerDescriptor, ...]] = {}
        self._handlers: dict[type, tuple[HandlerDescriptor, ...]] = {}
        self._decorators: list[Callable[[HandlerDescriptor], HandlerDescriptor]] = []

    @override
    def register(
        self,
        message_type: type,
        handler: Handler | type,
        name: str | None = None,
    ) -> HandlerDescriptor:
        """Bind ``handler`` to ``message_type`` and return its descriptor.

        Raises:
            HandlerSignatureError: If the handler's parameters are not a
                shape the bus can call.
        """
        declared = HandlerDescriptor.of(handler, name)
        decorated = self._decorated(declared)
        self._declared[message_type] = (*self._declared.get(message_type, ()), declared)
        self._handlers[message_type] = (*self._handlers.get(message_type, ()), decorated)
        return decorated

    @override
    def handlers_for(self, message_type: type) -> tuple[HandlerDescriptor, ...]:
        """Return every handler bound to ``message_type`` or any of its bases.

        Most specific first, and a handler registered twice across the chain
        runs once.

        All of them, not the nearest ancestor's. Stopping at the first match
        meant registering a handler on a subclass silently switched off one
        registered on its base — an audit trail or a metric attached to a
        marker class would stop firing the moment someone handled one
        subclass specifically, with nothing to indicate it. Routing already
        accumulates the same way, so the two now agree.
        """
        found: list[HandlerDescriptor] = []
        seen: set[int] = set()
        for base in message_type.__mro__:
            for descriptor in self._handlers.get(base, ()):
                if id(descriptor.handler) in seen:
                    continue
                seen.add(id(descriptor.handler))
                found.append(descriptor)
        return tuple(found)

    @override
    def message_types(self) -> tuple[type, ...]:
        """Return every message type with at least one handler."""
        return tuple(self._handlers)

    def decorate(self, wrap: Callable[[HandlerDescriptor], HandlerDescriptor]) -> None:
        """Apply ``wrap`` to every handler, registered now or later.

        For wrapping handlers in something they should not have to know
        about — a container filling their parameters, a span, a timer.
        Declaration stays where it is; this changes what is called.

        Decorators apply in the order given, always to the handler as it was
        declared. One equal to a decorator already applied takes its place
        rather than stacking on it, which is how an integration rebinds every
        handler — to a new container, say — without the old binding lingering
        underneath.
        """
        if wrap in self._decorators:
            self._decorators[self._decorators.index(wrap)] = wrap
        else:
            self._decorators.append(wrap)
        self._handlers = {
            message_type: tuple(self._decorated(d) for d in descriptors)
            for message_type, descriptors in self._declared.items()
        }

    def _decorated(self, descriptor: HandlerDescriptor) -> HandlerDescriptor:
        for wrap in self._decorators:
            descriptor = wrap(descriptor)
        return descriptor
