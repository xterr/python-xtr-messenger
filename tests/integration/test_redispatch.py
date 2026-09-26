"""A redispatch handled on a worker goes out through routing, end to end."""

from __future__ import annotations

from dataclasses import dataclass
from uuid import UUID, uuid4

import pytest

from xtr_messenger import (
    HandledStamp,
    HandlersLocator,
    InMemoryTransport,
    InMemoryTransportFactory,
    MessageBusConfig,
    MessageBusFactory,
    ReceivedStamp,
    RedispatchMessage,
    TransportConfig,
    WorkerFactory,
    as_message,
    as_message_handler,
)

pytestmark = pytest.mark.anyio


@as_message(name="test.integration.redispatch.report.v1")
@dataclass(frozen=True, slots=True)
class BuildReport:
    report_id: UUID


def _config() -> MessageBusConfig:
    return MessageBusConfig(
        transports={
            "generated": TransportConfig("in-memory://"),
            "reports": TransportConfig("in-memory://"),
            "urgent": TransportConfig("in-memory://"),
        },
        routing={RedispatchMessage: "generated", BuildReport: "reports"},
    )


async def test_a_worker_forwards_a_redispatch_to_where_routing_sends_it() -> None:
    factories = [InMemoryTransportFactory()]
    config = _config()
    report = BuildReport(uuid4())
    _ = await MessageBusFactory(config, factories).bus().dispatch(RedispatchMessage(report))

    await WorkerFactory(config, factories).worker(["generated"]).run()

    built = factories[0].create(config.transports)
    reports, generated = built["reports"], built["generated"]
    assert isinstance(reports, InMemoryTransport)
    assert isinstance(generated, InMemoryTransport)
    assert reports.messages == (report,)
    assert generated.rejected == ()


async def test_named_transports_override_routing() -> None:
    factories = [InMemoryTransportFactory()]
    config = _config()
    report = BuildReport(uuid4())
    bus = MessageBusFactory(config, factories).bus()
    _ = await bus.dispatch(RedispatchMessage(report, "urgent"))

    await WorkerFactory(config, factories).worker(["generated"]).run()

    built = factories[0].create(config.transports)
    urgent, reports = built["urgent"], built["reports"]
    assert isinstance(urgent, InMemoryTransport)
    assert isinstance(reports, InMemoryTransport)
    assert urgent.messages == (report,)
    assert reports.messages == ()


async def test_a_publishing_bus_handles_a_received_redispatch_through_itself() -> None:
    """A redispatch arriving received — from sync://, say — goes out through the
    same bus, and a handled message's result comes back."""
    handlers = HandlersLocator()

    @as_message_handler(BuildReport, handlers)
    async def build(message: BuildReport) -> str:
        return f"built {message.report_id}"

    report = BuildReport(uuid4())
    config = MessageBusConfig(transports={}, handle_unrouted=True)
    bus = MessageBusFactory(config, [InMemoryTransportFactory()], handlers).bus()

    handled = await bus.dispatch(RedispatchMessage(report), ReceivedStamp("sync"))

    [redispatched] = handled.all(HandledStamp)
    assert redispatched == HandledStamp("RedispatchMessageHandler", f"built {report.report_id}")
