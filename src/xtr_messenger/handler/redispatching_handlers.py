"""Handlers that also know how to handle a :class:`~xtr_messenger.message.RedispatchMessage`."""

from __future__ import annotations

from typing import TYPE_CHECKING, final

from typing_extensions import override

from xtr_messenger.message import RedispatchMessage

from .handler_descriptor import HandlerDescriptor
from .handlers_locator_interface import HandlersLocatorInterface
from .redispatch_message_handler import RedispatchMessageHandler

if TYPE_CHECKING:
    from collections.abc import Callable

    from xtr_messenger.message_bus_interface import MessageBusInterface

    from .handler_descriptor import Handler

__all__ = ["RedispatchingHandlers"]


@final
class RedispatchingHandlers(HandlersLocatorInterface):
    """``inner``'s handlers, plus one for :class:`RedispatchMessage` when it has none.

    A redispatch must go through a bus that routes, which only whoever builds
    the buses knows — so the factories supply the handler rather than a
    declaration. A handler ``inner`` already has for the message wins, which is
    how a container supplying its own is never doubled by this one: two
    handlers would dispatch the envelope twice.

    ``bus`` is called on the first redispatch, not before, so a worker that
    never redispatches never builds the publishing bus — nor opens its
    connections.
    """

    __slots__ = ("_built", "_bus", "_descriptor", "_inner")

    def __init__(
        self,
        inner: HandlersLocatorInterface,
        bus: Callable[[], MessageBusInterface],
    ) -> None:
        """Serve ``inner``'s handlers, redispatching through the bus ``bus`` returns."""
        self._inner = inner
        self._bus = bus
        self._built: RedispatchMessageHandler | None = None
        self._descriptor = HandlerDescriptor.of(self._redispatch, name="RedispatchMessageHandler")

    @override
    def register(
        self,
        message_type: type,
        handler: Handler | type,
        name: str | None = None,
    ) -> HandlerDescriptor:
        """Register on ``inner``, which is where handlers live."""
        return self._inner.register(message_type, handler, name)

    @override
    def handlers_for(self, message_type: type) -> tuple[HandlerDescriptor, ...]:
        """Return ``inner``'s handlers, or the redispatching one for a redispatch it lacks."""
        found = self._inner.handlers_for(message_type)
        if found or not issubclass(message_type, RedispatchMessage):
            return found
        return (self._descriptor,)

    @override
    def message_types(self) -> tuple[type, ...]:
        """Return ``inner``'s message types, and :class:`RedispatchMessage`."""
        known = self._inner.message_types()
        return known if RedispatchMessage in known else (*known, RedispatchMessage)

    async def _redispatch(self, message: RedispatchMessage) -> object:
        if self._built is None:
            self._built = RedispatchMessageHandler(self._bus())
        return await self._built(message)
