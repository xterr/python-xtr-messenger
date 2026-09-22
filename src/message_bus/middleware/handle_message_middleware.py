"""Invokes the handlers registered for a message."""

from __future__ import annotations

from typing import TYPE_CHECKING, final

from typing_extensions import override

from message_bus.exception import NoHandlerForMessageError
from message_bus.handler import default_registry
from message_bus.stamp import HandledStamp

from .middleware_interface import MiddlewareInterface

if TYPE_CHECKING:
    from message_bus.envelope import Envelope
    from message_bus.handler import HandlersLocatorInterface

    from .stack_interface import StackInterface

__all__ = ["HandleMessageMiddleware"]


@final
class HandleMessageMiddleware(MiddlewareInterface):
    """Runs every handler bound to the message, stamping each one.

    This is the end of the line for a message that stays in this process.
    Put it last: :class:`SendMessageMiddleware` short-circuits when a message
    was routed to a transport, so anything reaching here was either not
    routed anywhere or arrived *from* a transport and is meant to be handled
    now.

    That pairing is what makes one bus serve both sides. The publishing
    process routes and stops; the worker receives, finds a
    :class:`~message_bus.stamp.ReceivedStamp` that keeps the message from
    being published again, falls through, and handles it here.

    A message with no handler raises by default rather than passing quietly,
    because in a worker the overwhelmingly likely cause is an unimported
    handler module — and the quiet alternative is acknowledging the message
    and losing the work. Pass ``require_handler=False`` where a message
    genuinely may go unhandled, such as a bus shared by processes that
    subscribe to different subsets.
    """

    __slots__ = ("_registry", "_require_handler")

    def __init__(
        self,
        registry: HandlersLocatorInterface | None = None,
        *,
        require_handler: bool = True,
    ) -> None:
        """Resolve handlers from ``registry``, defaulting to the process-wide one."""
        self._registry = registry if registry is not None else default_registry()
        self._require_handler = require_handler

    @override
    async def handle(self, envelope: Envelope, stack: StackInterface) -> Envelope:
        """Invoke each bound handler, then continue down the chain.

        Raises:
            NoHandlerForMessageError: If nothing is bound and handlers are required.
        """
        message_type = type(envelope.message)
        descriptors = self._registry.handlers_for(message_type)
        if not descriptors and self._require_handler:
            raise NoHandlerForMessageError(message_type, self._handled_type_names())

        for descriptor in descriptors:
            await descriptor.invoke(envelope)
            envelope = envelope.with_stamps(HandledStamp(descriptor.name))
        return await stack.next().handle(envelope, stack)

    def _handled_type_names(self) -> tuple[str, ...]:
        return tuple(sorted(t.__qualname__ for t in self._registry.message_types()))
