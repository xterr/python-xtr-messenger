from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING

import pytest
from xtr_console import Application, CommandTester

from tests.support.fakes import RecordingBus
from tests.support.listing import BuildingInvoker, ListedTransport, ListedTransportFactory
from xtr_messenger import (
    Envelope,
    ErrorDetailsStamp,
    FailedMessageRepository,
    HandlersLocator,
    InMemoryTransportFactory,
    MessageBusConfig,
    SentToFailureTransportStamp,
    TransportConfig,
    TransportLocator,
    WorkerFactory,
    as_message,
    as_message_handler,
)
from xtr_messenger.command import (
    DebugCommand,
    FailedMessagesRemoveCommand,
    FailedMessagesRetryCommand,
    FailedMessagesShowCommand,
    SetupTransportsCommand,
    ShowMessagesCommand,
    StatsCommand,
)

if TYPE_CHECKING:
    from collections.abc import Callable


@as_message(name="tests.command.order_placed.v1")
@dataclass(frozen=True, slots=True)
class OrderPlaced:
    """An order was placed."""

    order_id: str


@as_message(name="tests.command.invoice_sent.v1", transport="mem")
@dataclass(frozen=True, slots=True)
class InvoiceSent:
    invoice_id: str


FAILED_AT = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)


def failed(message: object, at: datetime = FAILED_AT) -> Envelope:
    """``message`` as a transport keeps it once moved to its failure transport."""
    return Envelope(
        message,
        (
            ErrorDetailsStamp("RuntimeError", "boom"),
            SentToFailureTransportStamp("orders", at),
        ),
    )


@dataclass
class World:
    """Everything a command is built with, and what the handler saw."""

    orders: ListedTransport = field(default_factory=ListedTransport)
    failed: ListedTransport = field(default_factory=ListedTransport)
    handled: list[str] = field(default_factory=list)
    fail_handling: bool = False
    bus: RecordingBus = field(default_factory=RecordingBus)

    def config(self) -> MessageBusConfig:
        return MessageBusConfig(
            transports={
                "orders": TransportConfig("listed://"),
                "failed": TransportConfig("listed://"),
                "mem": TransportConfig("in-memory://"),
            },
            routing={OrderPlaced: "orders"},
            failure_transport="failed",
        )

    def tester(self, name: str) -> CommandTester:
        config = self.config()
        factories = [
            ListedTransportFactory({"orders": self.orders, "failed": self.failed}),
            InMemoryTransportFactory(),
        ]
        transports = TransportLocator(config, factories)
        repository = FailedMessageRepository(
            transports, self.bus, config.failure_transport_names(), config.failure_transport
        )
        handlers = HandlersLocator()

        @as_message_handler(OrderPlaced, handlers)
        async def handle(message: OrderPlaced) -> None:
            if self.fail_handling:
                raise RuntimeError("still failing")
            self.handled.append(message.order_id)

        workers = WorkerFactory(config, factories, handlers)
        services: dict[type, Callable[[], object]] = {
            SetupTransportsCommand: lambda: SetupTransportsCommand(transports),
            StatsCommand: lambda: StatsCommand(transports),
            ShowMessagesCommand: lambda: ShowMessagesCommand(transports),
            DebugCommand: lambda: DebugCommand(config, handlers),
            FailedMessagesShowCommand: lambda: FailedMessagesShowCommand(repository),
            FailedMessagesRemoveCommand: lambda: FailedMessagesRemoveCommand(repository),
            FailedMessagesRetryCommand: lambda: FailedMessagesRetryCommand(repository, workers),
        }
        application = Application(catch_exceptions=False)
        application.use_invoker(BuildingInvoker(services))
        return CommandTester(application, name, width=200)


@pytest.fixture
def world() -> World:
    return World()
