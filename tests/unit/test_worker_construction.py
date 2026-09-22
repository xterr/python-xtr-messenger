from __future__ import annotations

from typing import TYPE_CHECKING, final

import pytest
from typing_extensions import override

from message_bus import (
    Dsn,
    Envelope,
    MessageBusConfig,
    NotConsumableError,
    TransportConfig,
    Worker,
    WorkerFactory,
    WorkerProvidingInterface,
)
from message_bus.bridge.amqp import AmqpTransportFactory
from message_bus.transport.in_memory import InMemoryTransportFactory
from message_bus.transport.sender import SenderInterface
from message_bus.transport.sync import SyncTransportFactory
from message_bus.transport.transport_factory_interface import TransportFactoryInterface

if TYPE_CHECKING:
    from collections.abc import Mapping


@final
class SendOnlySender(SenderInterface):
    """A transport that publishes and cannot be consumed."""

    @override
    async def send(self, envelope: Envelope) -> Envelope:
        return envelope


@final
class SendOnlyFactory(TransportFactoryInterface):
    """An adapter that forgot to say how its transport is consumed."""

    @override
    def supports(self, dsn: Dsn) -> bool:
        return dsn.scheme == "send-only"

    @override
    def create(self, group: Mapping[str, TransportConfig]) -> Mapping[str, SenderInterface]:
        return dict.fromkeys(group, SendOnlySender())


def a_config(dsn: str) -> MessageBusConfig:
    return MessageBusConfig(transports={"t": TransportConfig(dsn)})


def test_building_a_worker_is_not_the_transport_factory_s_job() -> None:
    """Building a worker is not a transport factory's job: a whole transport
    is driven by the library's own loop."""
    assert not hasattr(TransportFactoryInterface, "worker")


def test_a_whole_transport_is_driven_by_the_library_s_own_loop() -> None:
    for dsn in ("sync://", "in-memory://"):
        assert isinstance(WorkerFactory(a_config(dsn)).worker(["t"]), Worker)


def test_a_broker_that_brings_its_own_worker_is_asked_for_it() -> None:
    config = a_config("amqp://guest:guest@localhost:5672/?queue=jobs")

    worker = WorkerFactory(config).worker(["t"])

    assert not isinstance(worker, Worker)
    assert type(worker).__name__ == "TaskiqWorker"


def test_only_the_adapter_that_needs_it_declares_the_extra_contract() -> None:
    assert isinstance(AmqpTransportFactory(), WorkerProvidingInterface)
    assert not isinstance(SyncTransportFactory(), WorkerProvidingInterface)
    assert not isinstance(InMemoryTransportFactory(), WorkerProvidingInterface)


def test_a_send_only_transport_that_brings_no_worker_says_so() -> None:
    """Neither consumable nor self-consuming leaves nothing to run."""
    config = a_config("send-only://")

    with pytest.raises(NotConsumableError, match="WorkerProvidingInterface"):
        _ = WorkerFactory(config, [SendOnlyFactory()]).worker(["t"])


def test_several_recorders_on_one_connection_drain_through_one_loop() -> None:
    config = MessageBusConfig(
        transports={
            "a": TransportConfig("in-memory://"),
            "b": TransportConfig("in-memory://"),
        },
    )

    worker = WorkerFactory(config).worker(["a", "b"])

    assert isinstance(worker, Worker)
