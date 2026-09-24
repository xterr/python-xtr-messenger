"""One structured log record per dispatch."""

from __future__ import annotations

from typing import TYPE_CHECKING, final

from typing_extensions import override
from xtr_logging import NullLogger

from xtr_messenger.stamp import SentStamp, TransportMessageIdStamp

from .middleware_interface import MiddlewareInterface

if TYPE_CHECKING:
    from xtr_logging import LoggerInterface

    from xtr_messenger.envelope import Envelope

    from .stack_interface import StackInterface

__all__ = ["LoggingMiddleware"]


@final
class LoggingMiddleware(MiddlewareInterface):
    """Logs the outcome of each dispatch, after the rest of the chain ran.

    Placed first so it observes the fully stamped result. It never catches
    exceptions and never logs errors — the layer that raised owns reporting
    its own failure, and a swallowed exception here would hide it.

    What it writes is one record per dispatch, carrying the message type and
    whatever the chain stamped — the transport it went to, the id the broker
    gave it — as context, so a formatter renders them apart from the message.
    """

    __slots__ = ("_logger",)

    def __init__(self, logger: LoggerInterface | None = None) -> None:
        """Log through ``logger``, discarding records without one."""
        self._logger = logger if logger is not None else NullLogger()

    @override
    async def handle(self, envelope: Envelope, stack: StackInterface) -> Envelope:
        """Await the chain, record what happened, return the result untouched."""
        result = await stack.next().handle(envelope, stack)

        context = {"message_type": type(result.message).__name__}
        sent = result.last(SentStamp)
        if sent is not None:
            context["transport"] = sent.sender_alias
        transport_id = result.last(TransportMessageIdStamp)
        if transport_id is not None:
            context["message_id"] = transport_id.message_id

        self._logger.info("message dispatched", context)
        return result
