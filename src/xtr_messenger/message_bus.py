"""The bus loop.

The bus is deliberately dumb: it wraps the message and calls the first
middleware. Everything else — routing, transport hand-off, logging,
validation — is middleware, so adding a dispatch-side concern never changes
the bus or the port publishers depend on.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, final

from typing_extensions import override

from .envelope import Envelope
from .message_bus_interface import MessageBusInterface
from .middleware.stack_middleware import StackMiddleware

if TYPE_CHECKING:
    from collections.abc import Sequence

    from .middleware.middleware_interface import MiddlewareInterface
    from .stamp import StampInterface

__all__ = ["MessageBus"]


@final
class MessageBus(MessageBusInterface):
    """Dispatches messages through a composed middleware chain."""

    __slots__ = ("_middlewares",)

    def __init__(self, middlewares: Sequence[MiddlewareInterface]) -> None:
        """Snapshot ``middlewares`` so later mutation cannot affect a live bus."""
        self._middlewares: tuple[MiddlewareInterface, ...] = tuple(middlewares)

    @override
    async def dispatch(self, message: object, *stamps: StampInterface) -> Envelope:
        """Dispatch ``message`` through the chain and return the stamped envelope.

        A raw message is wrapped in a fresh envelope; an existing envelope is
        passed through with its stamps intact, so a previously stamped
        envelope can be re-dispatched.

        A fresh cursor is built per call, so the same chain serves any number
        of dispatches even though each cursor is single-use.
        """
        envelope = Envelope.wrap(message, stamps)
        stack = StackMiddleware(self._middlewares)
        return await stack.next().handle(envelope, stack)
