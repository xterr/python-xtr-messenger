"""The logging surface the bus writes through."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

__all__ = ["LoggerInterface"]


@runtime_checkable
class LoggerInterface(Protocol):
    """The logging surface the bus writes through.

    Satisfied by structlog and loguru loggers as they are; a standard-library
    logger is adapted by
    :class:`~message_bus.middleware.logging_middleware.LoggingMiddleware`.
    """

    def info(self, message: str, /, **fields: str) -> None:
        """Emit an informational record with structured fields."""
        ...
