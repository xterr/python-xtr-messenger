"""What a transport that can prepare what it needs ahead of time offers."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

__all__ = ["SetupableTransportInterface"]


@runtime_checkable
class SetupableTransportInterface(Protocol):
    """A transport that can create what it needs — a table, a queue — before it is used.

    What ``messenger:setup-transports`` calls, so a deployment can create
    everything up front and run with automatic setup off.
    """

    async def setup(self) -> None:
        """Create whatever the transport needs that does not exist yet."""
        ...
