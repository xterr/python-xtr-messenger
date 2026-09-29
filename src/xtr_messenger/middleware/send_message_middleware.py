"""Hands routed messages to their transports."""

from __future__ import annotations

from typing import TYPE_CHECKING, final

from typing_extensions import override

from xtr_messenger.exception import NoSenderForMessageError
from xtr_messenger.stamp import ReceivedStamp, SentStamp

from .middleware_interface import MiddlewareInterface

if TYPE_CHECKING:
    from xtr_messenger.envelope import Envelope
    from xtr_messenger.stamp import StampInterface
    from xtr_messenger.transport.sender import SendersLocatorInterface

    from .stack_interface import StackInterface

__all__ = ["SendMessageMiddleware"]


@final
class SendMessageMiddleware(MiddlewareInterface):
    """Sends an envelope to every transport it is routed to.

    Two rules carry the whole producer/consumer split:

    * An envelope carrying a :class:`~xtr_messenger.stamp.ReceivedStamp` came
      *from* a transport, so it is never routed again — otherwise a consumer
      would re-publish everything it consumes.
    * Once at least one sender accepted the envelope, the chain
      short-circuits. A message with a transport configured is handed off,
      not also handled in the dispatching process — unless a sender hands it
      back received, as ``sync://`` does, meaning "handle it here": then it
      continues down the chain to be handled like anything a worker receives.

    When nothing is routed the chain stops there, as it does on a bus the
    factory builds. Set ``handle_unrouted=True`` to continue instead, which
    lets a downstream handling middleware pick the message up in-process.

    Each sender of a fan-out is handed the envelope as it was before any of
    them, plus its own :class:`~xtr_messenger.stamp.SentStamp`: none sees
    what another added. What each added is on the envelope returned.
    """

    __slots__ = ("_handle_unrouted", "_locator", "_require_sender")

    def __init__(
        self,
        locator: SendersLocatorInterface,
        *,
        require_sender: bool = False,
        handle_unrouted: bool = False,
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
        added: list[StampInterface] = []
        for alias, sender in self._locator.senders_for(envelope):
            stamped = envelope.with_stamps(SentStamp(type(sender).__name__, alias))
            added.extend(_added_by(await sender.send(stamped), envelope))
            sent = True

        if sent:
            envelope = envelope.with_stamps(*added)
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


def _added_by(returned: Envelope, before: Envelope) -> tuple[StampInterface, ...]:
    """Return the stamps ``returned`` carries that ``before`` did not.

    A sender appends to what it was handed; one that rebuilt the envelope
    instead has every stamp of its own counted — each stamp ``before`` had
    accounts for one equal stamp only, so a repeat the sender added is kept.
    """
    count = len(before.stamps)
    if returned.stamps[:count] == before.stamps:
        return returned.stamps[count:]
    unmatched = list(before.stamps)
    added: list[StampInterface] = []
    for stamp in returned.stamps:
        if stamp in unmatched:
            unmatched.remove(stamp)
        else:
            added.append(stamp)
    return tuple(added)
