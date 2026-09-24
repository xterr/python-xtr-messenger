"""The contract for looking up which handlers consume a message."""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from .handler_descriptor import Handler, HandlerDescriptor

__all__ = ["HandlersLocatorInterface"]


@runtime_checkable
class HandlersLocatorInterface(Protocol):
    """Binds message types to the handlers that consume them.

    Both halves are here on purpose. Declaring a handler writes to a
    registry and consuming a message reads from one, but they are the same
    registry — splitting the contract would mean an application that supplies
    its own could satisfy only half of what the library does with it.
    """

    def register(
        self,
        message_type: type,
        handler: Handler | type,
        name: str | None = None,
    ) -> HandlerDescriptor:
        """Bind ``handler`` to ``message_type`` and return its descriptor."""
        ...

    def handlers_for(self, message_type: type) -> tuple[HandlerDescriptor, ...]:
        """Return the handlers bound to ``message_type`` or any of its bases."""
        ...

    def message_types(self) -> tuple[type, ...]:
        """Return every message type with at least one handler."""
        ...
