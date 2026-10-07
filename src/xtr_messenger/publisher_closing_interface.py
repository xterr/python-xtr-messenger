"""The contract for a transport factory whose senders open their own connections."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

__all__ = ["PublisherClosingInterface"]


@runtime_checkable
class PublisherClosingInterface(Protocol):
    """A transport factory that has connections of its own to release.

    Most transports hold nothing: a ``sync://`` send is a function call, and an
    ``in-memory://`` one appends to a list. A broker is different — a producer
    has to open a connection before its first publish, and nothing closes it
    afterwards, because the consuming side's run loop is what normally does
    that and a publisher has no loop.

    Implement this in that case only, and close exactly what *this* factory's
    senders opened. One process may run several applications, each with its own
    factory; closing more than your own would drop a connection another is
    still publishing through.
    """

    async def close_publishers(self) -> None:
        """Release every connection this factory's senders opened for publishing.

        Called as an application shuts down, and safe to call when nothing was
        ever published: there is simply nothing to close. A connection reopens
        on the next publish, so this is not a one-way door.
        """
        ...
