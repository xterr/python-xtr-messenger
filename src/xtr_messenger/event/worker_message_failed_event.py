"""Dispatched when handling a message on a worker raised."""

from __future__ import annotations

from typing import TYPE_CHECKING, final

from .abstract_worker_message_event import AbstractWorkerMessageEvent

if TYPE_CHECKING:
    from xtr_messenger.envelope import Envelope

__all__ = ["WorkerMessageFailedEvent"]


@final
class WorkerMessageFailedEvent(AbstractWorkerMessageEvent):
    """Handling a message raised ``error``.

    Dispatched before the transport is told. :attr:`will_retry` says whether
    the transport will deliver the message again; the library's own worker
    never retries, so there it is only ever set by a listener that arranges a
    retry itself.
    """

    def __init__(
        self,
        envelope: Envelope,
        receiver_name: str,
        error: Exception,
        *,
        will_retry: bool = False,
    ) -> None:
        """Describe ``envelope`` from ``receiver_name``, whose handling raised ``error``."""
        super().__init__(envelope, receiver_name)
        self._error = error
        self._will_retry = will_retry

    @property
    def error(self) -> Exception:
        """Return what handling the message raised."""
        return self._error

    @property
    def will_retry(self) -> bool:
        """Tell whether the message will be delivered again."""
        return self._will_retry

    def set_for_retry(self) -> None:
        """Record that the message will be delivered again."""
        self._will_retry = True
