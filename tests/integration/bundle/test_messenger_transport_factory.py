"""Integration test: an app-provided transport factory serves the bus and worker."""

from __future__ import annotations

from typing import TYPE_CHECKING
from uuid import uuid4

import pytest
from xtr_dependency_injection import Kernel

from xtr_messenger import Envelope, MessageBusInterface, WorkerFactory

if TYPE_CHECKING:
    from xtr_service_contracts import ContainerInterface

pytestmark = pytest.mark.anyio

_APP = "tests.fixtures.app_transport_factory"


async def test_an_app_registered_factory_serves_the_bus_and_the_worker() -> None:
    from tests.fixtures.app_transport_factory.handlers import CountLedger  # noqa: PLC0415
    from tests.fixtures.app_transport_factory.messages import CountJob  # noqa: PLC0415
    from tests.fixtures.app_transport_factory.transport import (  # noqa: PLC0415
        CountingTransportFactory,
    )

    kernel = Kernel(_APP, env="test")
    booted = await kernel.boot()
    try:
        container: ContainerInterface = booted.container
        bus = await container.get(MessageBusInterface)
        job_id = uuid4()

        _ = await bus.dispatch(Envelope(CountJob(job_id)))

        qualifier = f"{CountingTransportFactory.__module__}:{CountingTransportFactory.__qualname__}"
        factory = await container.get(CountingTransportFactory, qualifier)
        assert factory.created >= 1

        worker = (await container.get(WorkerFactory)).worker(["jobs"])
        await worker.run()

        ledger = await container.get(CountLedger)
        assert ledger.done == [job_id]
        assert factory.created >= 2
    finally:
        await booted.shutdown()
