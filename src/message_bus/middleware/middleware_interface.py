"""One link in the dispatch chain."""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from message_bus.envelope import Envelope

    from .stack_interface import StackInterface

__all__ = ["MiddlewareInterface"]


@runtime_checkable
class MiddlewareInterface(Protocol):
    """One link in the dispatch chain."""

    async def handle(self, envelope: Envelope, stack: StackInterface, /) -> Envelope:
        """Process ``envelope`` and return what to hand back upstream.

        Continue the chain with ``await stack.next().handle(envelope, stack)``
        and return its (optionally re-stamped) result. Return an envelope
        *without* calling ``stack.next()`` to short-circuit — everything
        downstream is then skipped. Raising propagates untouched.

        Arguments are passed positionally, so an implementation may name its
        parameters whatever reads best.
        """
        ...
