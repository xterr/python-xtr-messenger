"""Every message dispatched through the container's buses is one unit of work.

Needs the ``di`` extra, so the package's ``middleware`` module does not import
it; the bundle does.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, final

from typing_extensions import override
from xtr_dependency_injection import unit_of_work

from xtr_messenger.middleware.middleware_interface import MiddlewareInterface

if TYPE_CHECKING:
    from xtr_service_contracts import ContainerInterface

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
    another is handled joins the unit already open.
    """

    __slots__ = ("_container",)

    def __init__(self, container: ContainerInterface) -> None:
        """Open units of work on ``container``'s services."""
        self._container = container

    @override
    async def handle(self, envelope: Envelope, stack: StackInterface, /) -> Envelope:
        """Run the rest of the chain inside a unit of work."""
        async with unit_of_work(self._container):
            return await stack.next().handle(envelope, stack)
