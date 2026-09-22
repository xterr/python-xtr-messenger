"""The registry binding message types to their handlers."""

from __future__ import annotations

from typing import TYPE_CHECKING, final

from typing_extensions import override

from .handler_descriptor import HandlerDescriptor
from .handlers_locator_interface import HandlersLocatorInterface

if TYPE_CHECKING:
    from .handler_descriptor import Handler

__all__ = ["HandlersLocator"]


@final
class HandlersLocator(HandlersLocatorInterface):
    """Binds message types to the handlers that consume them.

    Lookup walks the message's method resolution order, so registering a
    base class or a shared marker class handles every subclass. The first
    ancestor with handlers wins — a subclass with its own handlers is not
    also handled by its parent's.
    """

    __slots__ = ("_handlers",)

    def __init__(self) -> None:
        """Start empty."""
        self._handlers: dict[type, tuple[HandlerDescriptor, ...]] = {}

    @override
    def register(
        self,
        message_type: type,
        handler: Handler,
        name: str | None = None,
    ) -> HandlerDescriptor:
        """Bind ``handler`` to ``message_type`` and return its descriptor.

        Raises:
            HandlerSignatureError: If the handler's parameters are not a
                shape the bus can call.
        """
        descriptor = HandlerDescriptor.of(handler, name)
        self._handlers[message_type] = (*self._handlers.get(message_type, ()), descriptor)
        return descriptor

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

    @override
    def bindings(self) -> tuple[tuple[type, HandlerDescriptor], ...]:
        """Return every ``(message_type, handler)`` pair, for wiring."""
        return tuple(
            (message_type, descriptor)
            for message_type, descriptors in self._handlers.items()
            for descriptor in descriptors
        )
