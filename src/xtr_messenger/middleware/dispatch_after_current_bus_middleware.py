"""Holds stamped messages back until the message being handled is done with."""

from __future__ import annotations

from contextvars import ContextVar
from typing import TYPE_CHECKING, final

from typing_extensions import override

from xtr_messenger.exception import DelayedMessageHandlingError
from xtr_messenger.stamp import DispatchAfterCurrentBusStamp

from .middleware_interface import MiddlewareInterface

if TYPE_CHECKING:
    from xtr_messenger.envelope import Envelope

    from .stack_interface import StackInterface

__all__ = ["DispatchAfterCurrentBusMiddleware"]


@final
class _Held:
    """The messages held back while one message is handled, each with the rest of its chain."""

    __slots__ = ("envelopes", "open")

    def __init__(self) -> None:
        self.envelopes: list[tuple[Envelope, StackInterface]] = []
        self.open = True


# What the message being handled in this context holds back. In a context
# variable, not on the middleware: a worker and the bus its handlers dispatch
# through are two chains, and what a handler dispatches must wait for the
# worker's message all the same.
_held: ContextVar[_Held | None] = ContextVar("xtr_messenger_dispatch_after_current_bus")


@final
class DispatchAfterCurrentBusMiddleware(MiddlewareInterface):
    """Dispatches messages stamped ``DispatchAfterCurrentBusStamp`` once the current one succeeded.

    The first message dispatched in a context is the current one: the
    stamped messages dispatched while it is handled are held back — the rest
    of their chain kept — and dispatched, in order, once its whole chain
    returned. If it raised, they are dropped. A stamped message dispatched
    with nothing being handled goes out at once.

    The chain puts it first, ahead of every configured middleware, so what
    those do around the current message — commit a transaction — is done
    before anything held back goes out.
    """

    __slots__ = ()

    @override
    async def handle(self, envelope: Envelope, stack: StackInterface, /) -> Envelope:
        """Hold ``envelope`` back, pass it on, or handle it as the current message.

        Raises:
            DelayedMessageHandlingError: When the current message succeeded
                and a message it held back then failed.
        """
        held = _held.get(None)
        current = held is not None and held.open
        if envelope.last(DispatchAfterCurrentBusStamp) is not None:
            envelope = envelope.without_stamps(DispatchAfterCurrentBusStamp)
            if current and held is not None:
                held.envelopes.append((envelope, stack))
                return envelope
        if current:
            return await stack.next().handle(envelope, stack)
        return await self._current(envelope, stack)

    @staticmethod
    async def _current(envelope: Envelope, stack: StackInterface) -> Envelope:
        held = _Held()
        token = _held.set(held)
        try:
            handled = await stack.next().handle(envelope, stack)
            errors: list[Exception] = []
            # A message dispatched here may hold back more: they join the end.
            while held.envelopes:
                waiting, rest = held.envelopes.pop(0)
                try:
                    _ = await rest.next().handle(waiting, rest)
                except Exception as error:  # noqa: BLE001 — every held message is tried; each failure is reported.
                    errors.append(error)
        finally:
            held.open = False
            held.envelopes.clear()
            _held.reset(token)
        if errors:
            raise DelayedMessageHandlingError(handled, errors) from errors[0]
        return handled
