"""The cursor that walks a middleware chain, and ends it."""

from __future__ import annotations

from typing import TYPE_CHECKING, final

from typing_extensions import override

from .middleware_interface import MiddlewareInterface

if TYPE_CHECKING:
    from collections.abc import Sequence

    from message_bus.envelope import Envelope

    from .stack_interface import StackInterface

__all__ = ["StackMiddleware"]


@final
class StackMiddleware(MiddlewareInterface):
    """A single-use cursor that is also its own end-of-chain sentinel.

    Walking past the last middleware returns ``self``, whose ``handle`` is the
    identity function. A middleware at the end of the chain can therefore
    delegate unconditionally instead of special-casing the tail, and an empty
    chain is a legal no-op dispatch rather than an error.
    """

    __slots__ = ("_middlewares", "_offset")

    def __init__(self, middlewares: Sequence[MiddlewareInterface]) -> None:
        """Start a cursor positioned before the first middleware."""
        self._middlewares = middlewares
        self._offset = 0

    def next(self) -> MiddlewareInterface:
        """Advance the cursor, returning the identity tail when exhausted."""
        if self._offset >= len(self._middlewares):
            return self
        middleware = self._middlewares[self._offset]
        self._offset += 1
        return middleware

    @override
    async def handle(self, envelope: Envelope, _stack: StackInterface, /) -> Envelope:
        """Return ``envelope`` unchanged — the tail of the chain."""
        return envelope
