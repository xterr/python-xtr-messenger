"""The contract for resolving where a message goes."""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from collections.abc import Iterator

    from xtr_messenger.envelope import Envelope

    from .sender_interface import SenderInterface

__all__ = ["SendersLocatorInterface"]


@runtime_checkable
class SendersLocatorInterface(Protocol):
    """Resolves which senders an envelope should go to."""

    def senders_for(self, envelope: Envelope) -> Iterator[tuple[str, SenderInterface]]:
        """Yield ``(transport_name, sender)`` pairs, most specific first."""
        ...

    def routed_type_names(self) -> tuple[str, ...]:
        """Return the routed message types, for error messages."""
        ...
