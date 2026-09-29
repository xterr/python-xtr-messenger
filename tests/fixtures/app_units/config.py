"""Everything handled in-process; one middleware named bare, then twice with arguments."""

from __future__ import annotations

from xtr_dependency_injection import configure

from xtr_messenger import MessageBusConfig

from .messages import Nested, Record


@configure
def messenger() -> MessageBusConfig:
    return MessageBusConfig(
        routing={},
        handle_unrouted=True,
        middleware=["labelled", {"labelled": {"label": "first"}}, {"labelled": {"times": 2}}],
    )


__all__ = ["Nested", "Record", "messenger"]
