"""Hands routed messages to their transports."""

from __future__ import annotations

from typing import TYPE_CHECKING, final

from typing_extensions import override

from message_bus.exception import NoSenderForMessageError
from message_bus.stamp import ReceivedStamp, SentStamp

from .middleware_interface import MiddlewareInterface

if TYPE_CHECKING:
    from message_bus.envelope import Envelope
    from message_bus.transport.sender import SendersLocatorInterface

    from .stack_interface import StackInterface

__all__ = ["SendMessageMiddleware"]


@final
class SendMessageMiddleware(MiddlewareInterface):
    """Sends an envelope to every transport it is routed to.

    Two rules carry the whole producer/consumer split:

    * An envelope carrying a :class:`~message_bus.stamp.ReceivedStamp` came
      *from* a transport, so it is never routed again — otherwise a consumer
      would re-publish everything it consumes.
    * Once at least one sender accepted the envelope, the chain
      short-circuits. A message with a transport configured is handed off,
      not also handled in the dispatching process — unless a sender hands it
      back received, as ``sync://`` does, meaning "handle it here": then it
      continues down the chain to be handled like anything a worker receives.

    When nothing is routed, the chain continues, which is what lets a
    downstream handling middleware pick the message up in-process. Set
    ``handle_unrouted=False`` to stop there instead.
    """

    __slots__ = ("_handle_unrouted", "_locator", "_require_sender")

    def __init__(
        self,
        locator: SendersLocatorInterface,
        *,
        require_sender: bool = False,
        handle_unrouted: bool = True,
    ) -> None:
        """Wire the locator, optionally failing dispatches that route nowhere."""
        self._locator = locator
        self._require_sender = require_sender
        self._handle_unrouted = handle_unrouted

    @override
    async def handle(self, envelope: Envelope, stack: StackInterface) -> Envelope:
        """Route and send, or fall through when the envelope is not routed."""
        if envelope.last(ReceivedStamp) is not None:
            return await stack.next().handle(envelope, stack)

        sent = False
        for alias, sender in self._locator.senders_for(envelope):
            stamped = envelope.with_stamps(SentStamp(type(sender).__name__, alias))
            envelope = await sender.send(stamped)
            sent = True

        if sent:
            if envelope.last(ReceivedStamp) is None:
                return envelope
            return await stack.next().handle(envelope, stack)
        if self._require_sender:
            raise NoSenderForMessageError(
                type(envelope.message),
                self._locator.routed_type_names(),
            )
        if not self._handle_unrouted:
            return envelope
        return await stack.next().handle(envelope, stack)
