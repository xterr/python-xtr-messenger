"""The contract publishers dispatch through."""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from .envelope import Envelope
    from .stamp import StampInterface

__all__ = ["MessageBusInterface"]


@runtime_checkable
class MessageBusInterface(Protocol):
    """The port publishers depend on.

    Call sites type against this instead of
    :class:`~message_bus.message_bus.MessageBus` so the middleware
    composition and the transports behind it stay an infrastructure concern.
    """

    async def dispatch(self, message: object, *stamps: StampInterface) -> Envelope:
        """Dispatch ``message`` and return the envelope the chain hands back."""
        ...
