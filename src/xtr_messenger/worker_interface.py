"""The contract for what a worker process runs."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

__all__ = ["WorkerInterface"]


@runtime_checkable
class WorkerInterface(Protocol):
    """Something a worker process can run until it is stopped.

    A worker is whatever consumes messages and gets them handled. That is
    deliberately vague about *how*, because the two ways differ so much: the
    library ships a loop that pulls from a
    :class:`~xtr_messenger.transport.receiver.receiver_interface.ReceiverInterface`,
    while a broker that brings its own worker — taskiq, for one — is wrapped
    to present this same face.

    Depending on this, rather than on either implementation, is what makes a
    broker replaceable. An entrypoint says ``await worker.run()`` — and
    ``worker.stop()`` from a signal handler — and never learns which of the
    two it got.
    """

    async def run(self) -> None:
        """Consume and handle messages until stopped, cancelled or exhausted."""
        ...

    def stop(self) -> None:
        """Ask a running worker to finish the message in hand and return.

        Safe to call before :meth:`run` or after it returns: a worker that is
        not running has nothing to wind down.
        """
        ...
