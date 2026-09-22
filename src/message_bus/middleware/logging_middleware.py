"""One structured log record per dispatch."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, final

from typing_extensions import override

from message_bus.stamp import SentStamp, TransportMessageIdStamp

from .logger_interface import LoggerInterface
from .middleware_interface import MiddlewareInterface

if TYPE_CHECKING:
    from message_bus.envelope import Envelope

    from .stack_interface import StackInterface

__all__ = ["LoggingMiddleware"]


@final
class _StdlibLogger(LoggerInterface):
    """Adapts a standard-library logger to :class:`LoggerInterface`."""

    __slots__ = ("_logger",)

    def __init__(self, logger: logging.Logger) -> None:
        self._logger = logger

    @override
    def info(self, message: str, /, **fields: str) -> None:
        """Log ``message``, passing structured fields through ``extra``."""
        self._logger.info(message, extra={"message_bus": fields})


@final
class LoggingMiddleware(MiddlewareInterface):
    """Logs the outcome of each dispatch, after the rest of the chain ran.

    Placed first so it observes the fully stamped result. It never catches
    exceptions and never logs errors — the layer that raised owns reporting
    its own failure, and a swallowed exception here would hide it.
    """

    __slots__ = ("_logger",)

    def __init__(self, logger: LoggerInterface | None = None) -> None:
        """Log through ``logger``, defaulting to this module's stdlib logger."""
        self._logger = logger if logger is not None else _StdlibLogger(logging.getLogger(__name__))

    @override
    async def handle(self, envelope: Envelope, stack: StackInterface) -> Envelope:
        """Await the chain, record what happened, return the result untouched."""
        result = await stack.next().handle(envelope, stack)

        fields = {"message_type": type(result.message).__name__}
        sent = result.last(SentStamp)
        if sent is not None:
            fields["transport"] = sent.sender_alias
        transport_id = result.last(TransportMessageIdStamp)
        if transport_id is not None:
            fields["message_id"] = transport_id.message_id

        self._logger.info("message dispatched", **fields)
        return result
