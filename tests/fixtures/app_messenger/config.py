"""The application's messenger config."""

from __future__ import annotations

from xtr_dependency_injection import configure

from xtr_messenger import MessageBusConfig, TransportConfig

from .messages import AuditEvent, DoWork


@configure
def messenger() -> MessageBusConfig:
    """Route both messages through ``sync://`` — handled in-process by the container."""
    return MessageBusConfig(
        transports={
            "sync": TransportConfig("sync://"),
            "jobs": TransportConfig("in-memory://"),
        },
        routing={DoWork: "sync", AuditEvent: "jobs"},
    )
