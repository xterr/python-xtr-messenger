"""Test helpers for the taskiq bridge. Imports taskiq: only bridge tests use it."""

from __future__ import annotations

import asyncio
import json
from typing import TYPE_CHECKING, final
from uuid import uuid4

from taskiq import AsyncBroker, TaskiqMessage, TaskiqMiddleware
from typing_extensions import override

from xtr_messenger.bridge.taskiq.labels import HEADERS_LABEL, RETRIES_LABEL

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Sequence

    from taskiq import BrokerMessage

    from xtr_messenger import EncodedEnvelope


@final
class RecordingMiddleware(TaskiqMiddleware):
    """Captures the labels of every message as it is published."""

    def __init__(self, labels: list[dict[str, object]]) -> None:
        super().__init__()
        self.labels = labels

    @override
    def pre_send(self, message: TaskiqMessage) -> TaskiqMessage:
        self.labels.append(dict(message.labels))
        return message


@final
class DeliveringBroker(AsyncBroker):
    """A broker whose ``listen`` yields the deliveries it was given, then parks.

    Parking is what keeps a worker consuming until a test stops it.
    """

    def __init__(self, deliveries: Sequence[TaskiqMessage] = ()) -> None:
        super().__init__()
        self._deliveries = tuple(deliveries)
        self._parked = asyncio.Event()

    @override
    async def kick(self, message: BrokerMessage) -> None:
        del message

    @override
    async def listen(self) -> AsyncGenerator[bytes, None]:
        for message in self._deliveries:
            yield self.formatter.dumps(message).message
        _ = await self._parked.wait()


def delivery(encoded: EncodedEnvelope, task_name: str) -> TaskiqMessage:
    """Return ``encoded`` as the broker delivers it to the task named ``task_name``."""
    return TaskiqMessage(
        task_id=uuid4().hex,
        task_name=task_name,
        labels={RETRIES_LABEL: 0, HEADERS_LABEL: json.dumps(encoded.headers)},
        args=[encoded.body],
        kwargs={},
    )
