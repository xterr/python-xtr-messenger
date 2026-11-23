"""A failure transport an in-memory one stands in for, and what reaches it."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING, cast

from xtr_clock import MockClock

from xtr_messenger import (
    FailureTransportSender,
    InMemoryTransport,
    InMemoryTransportFactory,
    MessageBusConfig,
    TransportConfig,
    TransportLocator,
)

if TYPE_CHECKING:
    from xtr_messenger.event import WorkerMessageFailedEvent

FAILED_AT = "2026-01-01T12:00:00+00:00"

FIRST_FAILED_AT = datetime(2025, 12, 31, 9, 0, tzinfo=UTC)
"""When a message already in a failure transport first failed."""


def failure_transport(
    *sources: str, failure: str = "failed"
) -> tuple[FailureTransportSender, InMemoryTransport]:
    """Return a sender failing ``sources`` to ``failure``, and the in-memory ``failure``."""
    config = MessageBusConfig(
        transports={
            **{name: TransportConfig("in-memory://") for name in sources},
            failure: TransportConfig("in-memory://"),
        },
        failure_transport=failure,
    )
    locator = TransportLocator(config, [InMemoryTransportFactory()])
    kept = cast("InMemoryTransport", locator.get(failure))
    return FailureTransportSender(config, locator, MockClock(FAILED_AT)), kept


def set_for_retry(event: WorkerMessageFailedEvent) -> None:
    """Listen as one arranging the retry itself does."""
    event.set_for_retry()
