"""Every message dispatched through the container's buses is one unit of work.

Needs the ``di`` extra, so the package's ``middleware`` module does not import
it; the bundle does.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, final

from typing_extensions import override

from xtr_messenger.middleware.middleware_interface import MiddlewareInterface
from xtr_messenger.stamp import ReceivedStamp

if TYPE_CHECKING:
    from xtr_dependency_injection import ScopeFactoryInterface

    from xtr_messenger.envelope import Envelope
    from xtr_messenger.middleware.stack_interface import StackInterface

__all__ = ["UnitOfWorkMiddleware"]


@final
class UnitOfWorkMiddleware(MiddlewareInterface):
    """Opens a unit of work around the rest of the chain, for one message.

    The bundle puts it ahead of the configured middleware on the bus and in
    every worker, so the middleware after it and every handler of the
    message share the unit's scoped services — one database session, say —
    released once the message is done with. A message dispatched while
    another is handled joins the unit already open; a message a worker
    received is new work, a unit of its own even inside one — a worker run
    by a command. The middleware holding messages back until the current one
    was handled comes before it, so each message held back is a unit of its
    own.
    """

    __slots__ = ("_scopes",)

    def __init__(self, scopes: ScopeFactoryInterface) -> None:
        """Open units of work through ``scopes`` — the one power, not the whole container."""
        self._scopes = scopes

    @override
    async def handle(self, envelope: Envelope, stack: StackInterface, /) -> Envelope:
        """Run the rest of the chain inside a unit of work."""
        received = envelope.last(ReceivedStamp) is not None
        async with self._scopes.unit_of_work(join=not received):
            return await stack.next().handle(envelope, stack)
