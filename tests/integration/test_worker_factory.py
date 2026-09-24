"""Publishing and draining across a shared in-memory transport, end to end."""

from __future__ import annotations

from dataclasses import dataclass
from uuid import UUID, uuid4

import pytest

from xtr_messenger import (
    ErrorDetailsStamp,
    HandlersLocator,
    InMemoryTransport,
    InMemoryTransportFactory,
    MessageBusConfig,
    MessageBusFactory,
    TransportConfig,
    WorkerFactory,
    as_message,
    as_message_handler,
)

pytestmark = pytest.mark.anyio


@as_message(name="test.integration.worker_factory.job.v1")
@dataclass(frozen=True, slots=True)
class WorkerJob:
    job_id: UUID
    poison: bool = False


async def test_published_messages_are_drained_and_handled_over_a_shared_transport() -> None:
    """The publishing bus and the worker are built from separate factories but
    the SAME ``InMemoryTransportFactory`` instance, so the worker drains the
    very queue the bus published to; one poison message is rejected with the
    reason attached, and the append-only record survives the draining."""
    done: list[UUID] = []
    handlers = HandlersLocator()

    @as_message_handler(WorkerJob, handlers)
    async def run(message: WorkerJob) -> None:
        if message.poison:
            raise ValueError("cannot handle this one")
        done.append(message.job_id)

    factories = [InMemoryTransportFactory()]
    config = MessageBusConfig(
        transports={"jobs": TransportConfig("in-memory://")},
        routing={WorkerJob: "jobs"},
    )
    bus = MessageBusFactory(config, factories).bus()
    first, second = uuid4(), uuid4()
    _ = await bus.dispatch(WorkerJob(job_id=first))
    _ = await bus.dispatch(WorkerJob(job_id=uuid4(), poison=True))
    _ = await bus.dispatch(WorkerJob(job_id=second))

    await WorkerFactory(config, factories, handlers=handlers).worker(["jobs"]).run()

    assert done == [first, second]
    recorder = factories[0].create(config.transports)["jobs"]
    assert isinstance(recorder, InMemoryTransport)
    assert len(recorder.rejected) == 1
    details = recorder.rejected[0].last(ErrorDetailsStamp)
    assert details is not None
    assert details.exception_class == "ValueError"
    assert details.exception_message == "cannot handle this one"
    assert len(recorder.sent) == 3
    assert recorder.pending == 0
