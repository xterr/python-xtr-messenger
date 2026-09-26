"""Events a worker dispatches about itself and about each message it handles.

A worker given an event dispatcher announces when it starts and stops, and,
for every message, that it was received and then that it was handled or that
handling failed. Listeners can add stamps, skip a message, or stop the worker.
"""

from __future__ import annotations

from .abstract_worker_message_event import AbstractWorkerMessageEvent
from .worker_message_failed_event import WorkerMessageFailedEvent
from .worker_message_handled_event import WorkerMessageHandledEvent
from .worker_message_received_event import WorkerMessageReceivedEvent
from .worker_running_event import WorkerRunningEvent
from .worker_started_event import WorkerStartedEvent
from .worker_stopped_event import WorkerStoppedEvent

__all__ = [
    "AbstractWorkerMessageEvent",
    "WorkerMessageFailedEvent",
    "WorkerMessageHandledEvent",
    "WorkerMessageReceivedEvent",
    "WorkerRunningEvent",
    "WorkerStartedEvent",
    "WorkerStoppedEvent",
]
