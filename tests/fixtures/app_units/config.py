"""In-process but for one queued message; a middleware named bare, then twice with arguments."""

from __future__ import annotations

from xtr_dependency_injection import configure

from xtr_messenger import MessageBusConfig, TransportConfig

from .messages import Nested, Queued, Record


@configure
def messenger() -> MessageBusConfig:
    return MessageBusConfig(
        transports={"jobs": TransportConfig("in-memory://")},
        routing={Queued: "jobs"},
        handle_unrouted=True,
        middleware=["labelled", {"labelled": {"label": "first"}}, {"labelled": {"times": 2}}],
    )


__all__ = ["Nested", "Record", "messenger"]
