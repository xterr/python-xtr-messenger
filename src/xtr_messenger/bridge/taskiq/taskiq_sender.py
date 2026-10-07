"""The taskiq send half of a transport."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, final

from taskiq.kicker import AsyncKicker
from typing_extensions import override

from xtr_messenger.exception import UnsupportedStampError
from xtr_messenger.message_registry import name_of
from xtr_messenger.stamp import DelayStamp, TransportMessageIdStamp
from xtr_messenger.transport.sender import SenderInterface
from xtr_messenger.transport.serialization import JsonSerializer

from .labels import DELAY_LABEL, HEADERS_LABEL, QUEUE_LABEL, RETRIES_LABEL

if TYPE_CHECKING:
    from taskiq import AsyncBroker, AsyncTaskiqTask

    from xtr_messenger.envelope import Envelope
    from xtr_messenger.transport.serialization import SerializerInterface

    from .started_brokers import StartedBrokers

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

    __slots__ = ("_broker", "_queue", "_serializer", "_started", "_supports_delay")

    def __init__(
        self,
        broker: AsyncBroker,
        started: StartedBrokers,
        serializer: SerializerInterface | None = None,
        queue: str | None = None,
        *,
        supports_delay: bool = True,
    ) -> None:
        """Publish through ``broker``, optionally pinning a named queue.

        ``started`` is where this sender records that it opened the broker's
        connection. Give every sender built for one owner the same one: senders
        sharing a broker then open it once between them, and whoever owns the
        registry can close exactly those connections again.

        ``supports_delay`` says whether ``broker`` can hold a message back.
        Built ``False`` — as the AMQP factory does when the delayed-message
        exchange is turned off — a publish carrying a
        :class:`~xtr_messenger.stamp.DelayStamp` is refused rather than sent
        without its delay.
        """
        self._broker = broker
        self._started = started
        self._serializer = serializer if serializer is not None else JsonSerializer()
        self._queue = queue
        self._supports_delay = supports_delay

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
        """Encode, publish, and stamp the envelope with the task id.

        Raises:
            UnsupportedStampError: If the envelope carries a
                :class:`~xtr_messenger.stamp.DelayStamp` and this sender's
                broker cannot hold a message back.
        """
        encoded = self._serializer.encode(envelope)
        await self._started.ensure_started(self._broker)
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
            if not self._supports_delay:
                # Only the AMQP factory builds this off, when the broker's
                # delayed-message exchange is disabled.
                raise UnsupportedStampError(type(delay).__name__, "amqp")
            labels[DELAY_LABEL] = delay.delay_ms / _MILLISECONDS_PER_SECOND
        return labels
