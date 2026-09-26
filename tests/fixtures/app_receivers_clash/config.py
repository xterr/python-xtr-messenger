"""The application's messenger config: one in-memory queue."""

from __future__ import annotations

from xtr_dependency_injection import configure

from xtr_messenger import MessageBusConfig, TransportConfig


@configure
def messenger() -> MessageBusConfig:
    """Configure one transport; the receiver is registered, not configured."""
    return MessageBusConfig(transports={"jobs": TransportConfig("in-memory://")})
