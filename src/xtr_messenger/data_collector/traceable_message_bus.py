"""A bus that notes, for the profiled request, every message dispatched through it."""

from __future__ import annotations

from typing import TYPE_CHECKING, final, override

from xtr_clock import MonotonicClock
from xtr_http_kernel.data_collector import clone_var

from xtr_messenger._text import safe_str
from xtr_messenger.envelope import Envelope
from xtr_messenger.exception.handlers_failed_error import HandlersFailedError
from xtr_messenger.message_bus_interface import MessageBusInterface
from xtr_messenger.stamp.handled_stamp import HandledStamp
from xtr_messenger.stamp.sent_stamp import SentStamp

from ._masking import masked
from .dispatched_message import DispatchedMessage

if TYPE_CHECKING:
    from collections.abc import Mapping

    from xtr_clock import ClockInterface, DatePoint
    from xtr_http_kernel.data_collector import RequestTrace

    from xtr_messenger.stamp import StampInterface

__all__ = ["TraceableMessageBus"]

_MESSAGE_DEPTH = 2


@final
class TraceableMessageBus(MessageBusInterface):
    """Dispatches through another bus, recording each dispatch into a request trace.

    What is recorded is the message, the stamps it went in and came out with,
    the handlers that ran and what they returned, the senders that took it,
    how long the bus took, and what it raised — the error goes on unchanged.
    Outside a profiled request the trace records nothing, and the bus does no
    more than forward the call.
    """

    __slots__ = ("_bus", "_clock", "_trace")

    def __init__(
        self,
        bus: MessageBusInterface,
        trace: RequestTrace[DispatchedMessage],
        clock: ClockInterface | None = None,
    ) -> None:
        """Forward to ``bus``, recording into ``trace`` and timing with ``clock``.

        Args:
            bus: The bus every message is actually dispatched through.
            trace: Where each dispatch of a profiled request is recorded.
            clock: What the dispatch is timed with;
                :class:`~xtr_clock.MonotonicClock` by default, as a wall clock
                may jump in the middle of one.
        """
        self._bus = bus
        self._trace = trace
        self._clock: ClockInterface = clock if clock is not None else MonotonicClock()

    @override
    async def dispatch(self, message: object, *stamps: StampInterface) -> Envelope:
        """Dispatch ``message`` through the wrapped bus, recording it when profiled."""
        if not self._trace.is_recording():
            return await self._bus.dispatch(message, *stamps)
        given = Envelope.wrap(message, stamps)
        record = DispatchedMessage(
            message_class=_qualified_name(given.message),
            message=masked(clone_var(given.message, max_depth=_MESSAGE_DEPTH)),
            stamps_in=_stamp_names(given),
        )
        self._trace.record(record)
        started = self._clock.now()
        try:
            envelope = await self._bus.dispatch(message, *stamps)
        except BaseException as error:
            record.duration_ms = self._elapsed_ms(started)
            record.error = _error(error)
            if isinstance(error, HandlersFailedError):
                _settle(record, error.envelope)
            raise
        record.duration_ms = self._elapsed_ms(started)
        _settle(record, envelope)
        return envelope

    def _elapsed_ms(self, started: DatePoint) -> float:
        return (self._clock.now() - started).total_seconds() * 1000.0


def _settle(record: DispatchedMessage, envelope: Envelope) -> None:
    """Fill in what the envelope the bus handed back tells of the dispatch."""
    record.stamps_out = _stamp_names(envelope)
    record.handled = tuple(
        {
            "handler": stamp.handler_name,
            "result": masked(clone_var(stamp.result, max_depth=_MESSAGE_DEPTH)),
        }
        for stamp in envelope.all(HandledStamp)
    )
    record.sent = tuple(
        {"sender": stamp.sender_class, "alias": stamp.sender_alias}
        for stamp in envelope.all(SentStamp)
    )


def _error(error: BaseException) -> dict[str, object]:
    """Describe what the dispatch raised, and what each failed handler raised, as text."""
    handlers: Mapping[str, Exception] = (
        error.errors if isinstance(error, HandlersFailedError) else {}
    )
    return {
        "class": _qualified_name(error),
        "message": safe_str(error),
        "handlers": [
            {"handler": name, "class": _qualified_name(failure), "message": safe_str(failure)}
            for name, failure in handlers.items()
        ],
    }


def _stamp_names(envelope: Envelope) -> tuple[str, ...]:
    return tuple(type(stamp).__qualname__ for stamp in envelope.stamps)


def _qualified_name(value: object) -> str:
    kind = type(value)
    return f"{kind.__module__}.{kind.__qualname__}"
