"""Sending a message that failed for good to the failure transport configured for it."""

from __future__ import annotations

import logging
from datetime import UTC
from typing import TYPE_CHECKING, Final, final

from xtr_clock import SystemClock

from xtr_messenger.stamp import (
    DelayStamp,
    NonSendableStampInterface,
    RedeliveryStamp,
    SentToFailureTransportStamp,
    TransportMessageIdStamp,
)
from xtr_messenger.transport.sender.sender_interface import SenderInterface

if TYPE_CHECKING:
    from xtr_clock import ClockInterface

    from xtr_messenger.envelope import Envelope
    from xtr_messenger.message_bus_config import MessageBusConfig
    from xtr_messenger.transport.transport_locator import TransportLocator

__all__ = ["FailureTransportSender"]

#: Through the messenger channel, beside the rest of the bus's output.
_LOGGER: Final = logging.getLogger("messenger")


@final
class FailureTransportSender:
    """Sends a message that failed for good to its transport's failure transport.

    What every worker asks once a message's last attempt failed: when the
    transport it came from has a ``failure_transport`` — its own, or the
    configuration's — the message is sent there, stamped with a
    :class:`~xtr_messenger.stamp.SentToFailureTransportStamp` naming that
    transport and when, and the worker acknowledges the original. With none,
    the worker rejects it, and the transport does with it what it does with
    any failure.

    It travels as a new message: its attempt count, its delay and the id it
    was kept under where it failed are left behind, and so is what only this
    process stamped on it, so the failure transport gives it attempts of its
    own.

    A failure transport is where a message stops: one that already carries a
    :class:`~xtr_messenger.stamp.SentToFailureTransportStamp` — it failed in a
    failure transport, whether consumed there or retried from it — is never
    sent on. It is rejected where it is, and keeps the stamp naming where it
    first failed; two failure transports naming each other cannot pass a
    message back and forth.
    """

    __slots__ = ("_clock", "_config", "_transport_locator")

    def __init__(
        self,
        config: MessageBusConfig,
        transport_locator: TransportLocator,
        clock: ClockInterface | None = None,
    ) -> None:
        """Send to the failure transports ``config`` names, reached through ``transport_locator``.

        Args:
            config: Which transport fails to which.
            transport_locator: Where each failure transport is built, by name.
            clock: What reads the time a message failed at; the system clock
                when left out.
        """
        self._config = config
        self._transport_locator = transport_locator
        self._clock: ClockInterface = clock if clock is not None else SystemClock()

    def failure_transport_of(self, receiver_name: str) -> str | None:
        """Return where what fails for good on ``receiver_name`` is sent, if anywhere."""
        return self._config.failure_transport_of(receiver_name)

    async def send(self, envelope: Envelope, receiver_name: str) -> bool:
        """Send ``envelope``, failed for good on ``receiver_name``, to its failure transport.

        Returns ``False`` — and sends nothing — when the transport has no
        failure transport, when ``envelope`` failed in a failure transport
        already, or when sending raised: the failure is logged, and the caller
        leaves the message to its own transport rather than lose it.
        """
        failure = self.failure_transport_of(receiver_name)
        if failure is None or envelope.last(SentToFailureTransportStamp) is not None:
            return False
        moved = (
            envelope.without_stamps(NonSendableStampInterface)
            .without_stamps(RedeliveryStamp)
            .without_stamps(DelayStamp)
            .without_stamps(TransportMessageIdStamp)
            .with_stamps(
                SentToFailureTransportStamp(receiver_name, self._clock.now().astimezone(UTC))
            )
        )
        try:
            sender = self._transport_locator.get(failure)
            if not isinstance(sender, SenderInterface):
                _LOGGER.error("the failure transport %r cannot be sent to", failure)
                return False
            _ = await sender.send(moved)
        # Logged, and the message stays with its own transport rather than be lost.
        except Exception:
            _LOGGER.exception(
                "message %s could not be sent to the failure transport %r",
                type(envelope.message).__qualname__,
                failure,
            )
            return False
        _LOGGER.info(
            "message %s failed for good on %r and was sent to the failure transport %r",
            type(envelope.message).__qualname__,
            receiver_name,
            failure,
        )
        return True
