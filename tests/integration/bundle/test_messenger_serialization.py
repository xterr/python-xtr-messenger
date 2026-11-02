"""End-to-end: a message routed through a serializing transport arrives exactly as it left."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from xtr_dependency_injection import Kernel

from tests.support.messages import assert_dispatch_invoice_is_exact, dispatch_invoice
from xtr_messenger import (
    Envelope,
    MessageBusConfig,
    MessageBusInterface,
    TransportFactory,
)
from xtr_messenger.transport.in_memory import InMemoryTransport

if TYPE_CHECKING:
    from xtr_service_contracts import ContainerInterface

pytestmark = pytest.mark.anyio


async def test_a_dispatched_message_arrives_exactly_through_the_wire_format() -> None:
    booted = await Kernel("tests.fixtures.app_wire", env="test").boot()
    try:
        container: ContainerInterface = booted.container
        bus = await container.get(MessageBusInterface)
        config = await container.get(MessageBusConfig)
        transports = await container.get(TransportFactory)
        sent = dispatch_invoice()

        _ = await bus.dispatch(Envelope(sent))

        recorder = transports.create({"wire": config.transports["wire"]})["wire"]
        assert isinstance(recorder, InMemoryTransport)
        [received] = recorder.messages
        assert_dispatch_invoice_is_exact(received, sent)
    finally:
        await booted.shutdown()
