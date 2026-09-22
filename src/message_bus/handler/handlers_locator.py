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
        """Return the handlers bound to ``message_type`` or any of its bases."""
        for base in message_type.__mro__:
            found = self._handlers.get(base)
            if found:
                return found
        return ()

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
