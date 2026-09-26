"""What every event about one message on a worker carries."""

from __future__ import annotations

from typing import TYPE_CHECKING

from xtr_event_dispatcher_contracts import Event

if TYPE_CHECKING:
    from xtr_messenger.envelope import Envelope
    from xtr_messenger.stamp import StampInterface

__all__ = ["AbstractWorkerMessageEvent"]


class AbstractWorkerMessageEvent(Event):
    """An envelope a worker is dealing with, and the transport it came from.

    Listeners may add stamps. The worker carries on with :attr:`envelope` as
    the listeners left it, so a stamp added here reaches the handlers or the
    transport that settles the message.
    """

    def __init__(self, envelope: Envelope, receiver_name: str) -> None:
        """Describe ``envelope``, collected from the transport ``receiver_name``."""
        self._envelope: Envelope = envelope
        self._receiver_name: str = receiver_name

    @property
    def envelope(self) -> Envelope:
        """Return the envelope, with whatever stamps listeners added so far."""
        return self._envelope

    @property
    def receiver_name(self) -> str:
        """Return the name of the transport the envelope was collected from."""
        return self._receiver_name

    def add_stamps(self, *stamps: StampInterface) -> None:
        """Add ``stamps`` to the envelope the worker carries on with."""
        self._envelope = self._envelope.with_stamps(*stamps)
