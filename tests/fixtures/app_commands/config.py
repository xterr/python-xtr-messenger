"""The application's messenger config: a queue, its failure transport, a receiver."""

from __future__ import annotations

from xtr_dependency_injection import configure

from xtr_messenger import MessageBusConfig, TransportConfig

from .messages import Shipped


@configure
def messenger() -> MessageBusConfig:
    """Route shipments to ``jobs``; what fails for good there is kept in ``failed``."""
    return MessageBusConfig(
        transports={
            "jobs": TransportConfig("in-memory://"),
            "failed": TransportConfig("in-memory://"),
        },
        routing={Shipped: "jobs"},
        failure_transport="failed",
    )
