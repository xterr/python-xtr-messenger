"""The application's messenger config."""

from __future__ import annotations

from xtr_dependency_injection import configure

from tests.support.messages import DispatchInvoice
from xtr_messenger import MessageBusConfig, TransportConfig


@configure
def messenger() -> MessageBusConfig:
    """Route the invoice to a recorder that writes and reads back every message."""
    return MessageBusConfig(
        transports={"wire": TransportConfig("in-memory://?serialize=true")},
        routing={DispatchInvoice: "wire"},
    )
