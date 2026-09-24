"""The taskiq send half of a transport."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, final

from taskiq.kicker import AsyncKicker
from typing_extensions import override

from xtr_messenger.message_registry import name_of
from xtr_messenger.stamp import DelayStamp, TransportMessageIdStamp
from xtr_messenger.transport.sender import SenderInterface
from xtr_messenger.transport.serialization import JsonSerializer

from .broker import ensure_started
from .labels import HEADERS_LABEL, QUEUE_LABEL, RETRIES_LABEL

if TYPE_CHECKING:
    from taskiq import AsyncBroker, AsyncTaskiqTask

    from xtr_messenger.envelope import Envelope
    from xtr_messenger.transport.serialization import SerializerInterface

__all__ = ["TaskiqSender"]

_MILLISECONDS_PER_SECOND = 1000.0


@final
class TaskiqSender(SenderInterface):
    """Publishes envelopes to a taskiq broker, addressed by message name.

    The message name doubles as the task name, so a producer never imports
    the module that handles the message — it only needs the message class,
    which both sides already share.

    Every publish carries ``_retries=0``. That is not configurable on
    purpose: it makes a *missing* retry label anomalous, so a consumer can
    tell "first delivery" from "label was lost" rather than guessing.
    """

    __slots__ = ("_broker", "_queue", "_serializer")

    def __init__(
        self,
        broker: AsyncBroker,
        serializer: SerializerInterface | None = None,
        queue: str | None = None,
    ) -> None:
        """Publish through ``broker``, optionally pinning a named queue."""
        self._broker = broker
        self._serializer = serializer if serializer is not None else JsonSerializer()
        self._queue = queue

    @property
    def broker(self) -> AsyncBroker:
        """The broker this sender publishes through."""
        return self._broker

    @property
    def queue(self) -> str | None:
        """The queue this sender publishes to, if it pins one."""
        return self._queue

    @property
    def serializer(self) -> SerializerInterface:
        """The serializer this sender encodes with.

        Exposed so a deployment can confirm both halves of a transport were
        built with the same one — they run in different processes, and a
        producer encoding stamps a consumer will not restore is silent.
        """
        return self._serializer

    @override
    async def send(self, envelope: Envelope) -> Envelope:
        """Encode, publish, and stamp the envelope with the task id."""
        encoded = self._serializer.encode(envelope)
        await ensure_started(self._broker)
        kicker: AsyncKicker[..., None] = AsyncKicker(
            task_name=name_of(type(envelope.message)),
            broker=self._broker,
            labels=self._labels(envelope, encoded.headers),
        )
        handle: AsyncTaskiqTask[None] = await kicker.kiq(encoded.body)
        return envelope.with_stamps(TransportMessageIdStamp(handle.task_id))

    def _labels(self, envelope: Envelope, headers: dict[str, str]) -> dict[str, object]:
        labels: dict[str, object] = {
            RETRIES_LABEL: 0,
            HEADERS_LABEL: json.dumps(headers),
        }
        if self._queue is not None:
            labels[QUEUE_LABEL] = self._queue
        delay = envelope.last(DelayStamp)
        if delay is not None:
            labels["delay"] = delay.delay_ms / _MILLISECONDS_PER_SECOND
        return labels
