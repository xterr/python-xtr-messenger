from __future__ import annotations

import pytest

from xtr_messenger import Envelope, MessageBus
from xtr_messenger.bundle._profiler import traceable_message_bus
from xtr_messenger.data_collector import MessengerDataCollector

pytestmark = pytest.mark.anyio


async def test_the_traced_bus_records_into_the_collector_s_trace() -> None:
    collector = MessengerDataCollector()
    bus = traceable_message_bus(MessageBus(()), collector)
    collector.begin_request()

    envelope = await bus.dispatch("hello")

    assert envelope == Envelope("hello")
    assert [entry.message_class for entry in collector.trace.entries()] == ["builtins.str"]
