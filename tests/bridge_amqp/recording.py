from __future__ import annotations

from typing import TYPE_CHECKING, final

from taskiq import TaskiqMiddleware
from typing_extensions import override

if TYPE_CHECKING:
    from taskiq import TaskiqMessage


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
