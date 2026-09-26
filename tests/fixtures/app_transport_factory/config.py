"""The counting-transport fixture app's messenger config."""

from __future__ import annotations

from xtr_dependency_injection import configure

from xtr_messenger import MessageBusConfig, TransportConfig

from .messages import CountJob


@configure
def messenger() -> MessageBusConfig:
    """Route the job through the app-provided ``counting://`` transport."""
    return MessageBusConfig(
        transports={"jobs": TransportConfig("counting://")},
        routing={CountJob: "jobs"},
    )
