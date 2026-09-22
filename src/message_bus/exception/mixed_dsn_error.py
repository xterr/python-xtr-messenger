"""A worker was asked to serve transports on different servers."""

from __future__ import annotations

from .message_bus_error import MessageBusError

__all__ = ["MixedDsnError"]


class MixedDsnError(MessageBusError):
    """A worker was asked to serve transports that live on different brokers."""

    dsns: tuple[str, ...]

    def __init__(self, dsns: tuple[str, ...]) -> None:
        """Record the conflicting DSNs."""
        self.dsns = dsns
        remedy = "run one worker per DSN"
        super().__init__(f"a worker serves one broker, but got {len(dsns)} DSNs; {remedy}")
