"""Every message the request dispatched on the bus."""

from __future__ import annotations

from typing import TYPE_CHECKING, Final, final

from xtr_http_kernel.data_collector import RequestTrace

if TYPE_CHECKING:
    from xtr_http_kernel.data_collector import ProfiledRequest

    from .dispatched_message import DispatchedMessage

__all__ = ["MessengerDataCollector"]

_NAME: Final = "messenger"


@final
class MessengerDataCollector:
    """Keeps the messages the request dispatched, read off the trace a traced bus fills.

    The collector owns the trace and starts it as each profiled request
    arrives; a :class:`~xtr_messenger.data_collector.TraceableMessageBus`
    built on :attr:`trace` records into it. The trace is read once everything
    has been sent, so a message dispatched while the body streamed, or by a
    listener after it, is there too.
    """

    __slots__ = ("_trace",)

    def __init__(self) -> None:
        """Start with a trace of its own, empty in every request until one begins."""
        self._trace: RequestTrace[DispatchedMessage] = RequestTrace("messenger_dispatches")

    @property
    def name(self) -> str:
        """``"messenger"``."""
        return _NAME

    @property
    def trace(self) -> RequestTrace[DispatchedMessage]:
        """The trace a traced bus records each dispatch into."""
        return self._trace

    def begin_request(self) -> None:
        """Start recording the request that just arrived."""
        self._trace.begin()

    def collect(self, profiled: ProfiledRequest) -> dict[str, object]:
        """Return what was dispatched so far; the rest follows once the response has left."""
        del profiled
        return self._read()

    def late_collect(self, data: dict[str, object], profiled: ProfiledRequest) -> dict[str, object]:
        """Return every message the request dispatched."""
        del data, profiled
        return self._read()

    def _read(self) -> dict[str, object]:
        dispatched = self._trace.entries()
        return {
            "messages": [message.to_data() for message in dispatched],
            "count": len(dispatched),
            "handled": sum(len(message.handled) for message in dispatched),
            "sent": sum(len(message.sent) for message in dispatched),
            "failed": sum(1 for message in dispatched if message.error is not None),
            "duration_ms": sum(message.duration_ms or 0.0 for message in dispatched),
        }
