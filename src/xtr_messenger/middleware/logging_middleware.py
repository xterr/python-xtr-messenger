"""What a dispatch did, told in as much detail as the command asked for."""

from __future__ import annotations

from typing import TYPE_CHECKING, final

from typing_extensions import override
from xtr_logging import NullLogger

from xtr_messenger.stamp import HandledStamp, SentStamp, TransportMessageIdStamp

from .middleware_interface import MiddlewareInterface

if TYPE_CHECKING:
    from xtr_logging import Context, LoggerInterface

    from xtr_messenger.envelope import Envelope

    from .stack_interface import StackInterface

__all__ = ["LoggingMiddleware"]


@final
class LoggingMiddleware(MiddlewareInterface):
    """Logs the outcome of each dispatch, after the rest of the chain ran.

    Placed first so it observes the fully stamped result. It never catches
    exceptions and never logs errors — the layer that raised owns reporting
    its own failure, and a swallowed exception here would hide it.

    What it writes is graded by severity, so a console command turns ``-v``
    into how much of a dispatch it sees — xtr-logging's ``ConsoleHandler``
    prints notices at ``-v``, info at ``-vv`` and everything at ``-vvv``:

    ===============  ======  =========================================
    Level            Shown   Records
    ===============  ======  =========================================
    ``NOTICE``       ``-v``  One per dispatch: the message type, the
                             transport it went to and the id the broker
                             gave it.
    ``INFO``         ``-vv`` One per handler that ran, and what it
                             returned.
    ``DEBUG``        ``-vvv`` One per dispatch, carrying every stamp the
                             envelope came back with.
    ===============  ======  =========================================

    Nothing appears at normal verbosity: a dispatch is routine, and a bus
    running thousands a second should not say so unasked. Each tier is a
    distinct record adding what the one above it left out, never a repeat of
    it, and every value travels as context so a formatter renders it apart
    from the text.
    """

    __slots__ = ("_logger",)

    def __init__(self, logger: LoggerInterface | None = None) -> None:
        """Log through ``logger``, discarding records without one."""
        self._logger = logger if logger is not None else NullLogger()

    @override
    async def handle(self, envelope: Envelope, stack: StackInterface) -> Envelope:
        """Await the chain, record what happened, return the result untouched."""
        result = await stack.next().handle(envelope, stack)
        message_type = type(result.message).__name__

        self._logger.notice("message dispatched", _dispatched(result, message_type))
        for handled in result.all(HandledStamp):
            self._logger.info("message handled", _handled(handled, message_type))
        self._logger.debug("envelope stamped", _stamped(result, message_type))

        return result


def _dispatched(result: Envelope, message_type: str) -> Context:
    """Where the message went, as far as the chain got it."""
    context: dict[str, object] = {"message_type": message_type}
    sent = result.last(SentStamp)
    if sent is not None:
        context["transport"] = sent.sender_alias
    transport_id = result.last(TransportMessageIdStamp)
    if transport_id is not None:
        context["message_id"] = transport_id.message_id
    return context


def _handled(handled: HandledStamp, message_type: str) -> Context:
    """Which handler ran, and what it returned when it returned anything."""
    context: dict[str, object] = {"message_type": message_type, "handler": handled.handler_name}
    if handled.result is not None:
        context["result"] = handled.result
    return context


def _stamped(result: Envelope, message_type: str) -> Context:
    """Every stamp the envelope came back with, in the order they were added."""
    return {
        "message_type": message_type,
        "stamps": tuple(repr(stamp) for stamp in result.stamps),
    }
