"""What a receiver that retries a rejected message tells the worker ahead of the rejection."""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from xtr_messenger.envelope import Envelope

__all__ = ["RetryingReceiverInterface"]


@runtime_checkable
class RetryingReceiverInterface(Protocol):
    """A receiver that puts a rejected message back for another attempt while it has some left.

    The worker asks before it announces a failure, so a listener of the
    failed event — the one paging an administrator, say — can tell an attempt
    that will be made again from a message that failed for good.
    """

    def will_retry(self, envelope: Envelope) -> bool:
        """Tell whether rejecting ``envelope`` now puts it back for another attempt."""
        ...
