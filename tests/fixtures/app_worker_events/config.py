"""The application's messenger config: one in-memory queue."""

from __future__ import annotations

from xtr_dependency_injection import configure

from xtr_messenger import MessageBusConfig, TransportConfig

from .messages import Ping


@configure
def messenger() -> MessageBusConfig:
    """Route ``Ping`` to an in-memory queue a worker then drains."""
    return MessageBusConfig(
        transports={"jobs": TransportConfig("in-memory://")},
        routing={Ping: "jobs"},
    )
