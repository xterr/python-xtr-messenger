"""The profiler's messenger panel: every message a profiled request dispatched.

:class:`TraceableMessageBus` wraps the bus and records each dispatch — the
message, its stamps, the handlers and senders it reached, its duration, its
error — into the trace :class:`MessengerDataCollector` owns and reads back.
Needs xtr-http-kernel, whose profiler the collector plugs into; the bundle
registers both only while that profiler is on.
"""

from __future__ import annotations

from .dispatched_message import DispatchedMessage
from .messenger_data_collector import MessengerDataCollector
from .traceable_message_bus import TraceableMessageBus

__all__ = ["DispatchedMessage", "MessengerDataCollector", "TraceableMessageBus"]
