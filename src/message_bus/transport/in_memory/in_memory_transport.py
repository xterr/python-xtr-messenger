"""A transport that records instead of sending — for tests."""

from __future__ import annotations

from collections import deque
from typing import TYPE_CHECKING, final
from uuid import uuid4

from typing_extensions import override

from message_bus.stamp import AckReceiptStamp, ReceivedStamp, TransportMessageIdStamp
from message_bus.transport.transport_interface import TransportInterface

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from message_bus.envelope import Envelope
    from message_bus.transport.serialization import SerializerInterface

__all__ = ["InMemoryTransport"]


@final
class InMemoryTransport(TransportInterface):
    """Captures dispatched envelopes so a test can assert on them.

    Pass a ``serializer`` to round-trip every envelope through encode/decode
    on the way in. That costs nothing in a test and catches the class of bug
    where a message only fails once it meets a real transport — an
    unserializable field, or a name that will not resolve on the consumer.

    Mutable by design: recording is the whole point.

    **The record and the queue are separate on purpose.** :attr:`sent` is an
    append-only log that nothing drains, while :meth:`get` consumes from an
    independent queue that :meth:`send` also feeds. Were they one structure,
    running a worker would empty the very list the test then asserts on, and
    the assertion would pass or fail depending on scheduling. Keeping them
    apart means a single test can both drive a real receive loop and check
    what was published, without the two interfering.
    """

    __slots__ = ("_queue", "_rejected", "_sent", "_serializer", "_unsettled")

    def __init__(self, serializer: SerializerInterface | None = None) -> None:
        """Record into a fresh log, optionally round-tripping through ``serializer``."""
        self._sent: list[Envelope] = []
        self._queue: deque[Envelope] = deque()
        self._unsettled: dict[int, Envelope] = {}
        self._rejected: list[Envelope] = []
        self._serializer = serializer

    @property
    def sent(self) -> tuple[Envelope, ...]:
        """Return the envelopes recorded so far, in dispatch order.

        Never drained by :meth:`get`, so this stays a complete history even
        while a worker consumes.
        """
        return tuple(self._sent)

    @property
    def messages(self) -> tuple[object, ...]:
        """Return just the recorded messages, in dispatch order."""
        return tuple(envelope.message for envelope in self._sent)

    @property
    def rejected(self) -> tuple[Envelope, ...]:
        """Return the envelopes a consumer rejected, in rejection order."""
        return tuple(self._rejected)

    @property
    def pending(self) -> int:
        """Return how many envelopes are queued but not yet collected."""
        return len(self._queue)

    def clear(self) -> None:
        """Forget everything recorded, queued, outstanding, and rejected."""
        self._sent.clear()
        self._queue.clear()
        self._unsettled.clear()
        self._rejected.clear()

    @override
    async def send(self, envelope: Envelope) -> Envelope:
        """Record ``envelope``, queue it for collection, and stamp a message id."""
        recorded = envelope
        if self._serializer is not None:
            recorded = self._serializer.decode(self._serializer.encode(envelope))
        stamped = recorded.with_stamps(TransportMessageIdStamp(str(uuid4())))
        self._sent.append(stamped)
        self._queue.append(stamped)
        return envelope.with_stamps(*stamped.all(TransportMessageIdStamp))

    @override
    async def get(self) -> AsyncIterator[Envelope]:
        """Yield queued envelopes until the queue is empty.

        Stops rather than waiting for more, so a test can drive the loop to
        completion without having to cancel it.
        """
        receipt = 0
        while self._queue:
            envelope = self._queue.popleft()
            receipt += 1
            self._unsettled[receipt] = envelope
            yield envelope.with_stamps(ReceivedStamp("in-memory"), AckReceiptStamp(receipt))

    @override
    async def ack(self, envelope: Envelope) -> None:
        """Drop the outstanding record for ``envelope``."""
        _ = self._unsettled.pop(_receipt_of(envelope), None)

    @override
    async def reject(self, envelope: Envelope) -> None:
        """Record ``envelope`` as rejected and stop tracking it.

        Records the envelope as handed over, not as it was collected, so
        anything the consumer learned on the way — an
        :class:`~message_bus.stamp.ErrorDetailsStamp` saying why — is kept
        rather than discarded.
        """
        _ = self._unsettled.pop(_receipt_of(envelope), None)
        self._rejected.append(envelope)


def _receipt_of(envelope: Envelope) -> int:
    stamp = envelope.last(AckReceiptStamp)
    return stamp.receipt if stamp is not None else -1
