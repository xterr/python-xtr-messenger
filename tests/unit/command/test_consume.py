from __future__ import annotations

import asyncio
import os
import signal
import sys
from dataclasses import dataclass
from typing import TYPE_CHECKING, final
from uuid import UUID, uuid4

import pytest
from typing_extensions import override
from xtr_console import Application, CommandTester, ExitCode

from xtr_messenger import (
    Dsn,
    Envelope,
    HandlersLocator,
    InMemoryTransportFactory,
    MessageBusConfig,
    MessageBusFactory,
    TransportConfig,
    TransportFactoryInterface,
    TransportInterface,
    WorkerFactory,
    as_message,
    as_message_handler,
)
from xtr_messenger.command import ConsumeMessagesCommand

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterator, Mapping

pytestmark = pytest.mark.anyio


@as_message(name="test.command.consume.job.v1")
@dataclass(frozen=True, slots=True)
class ConsumeJob:
    job_id: UUID


@final
class IdleTransport(TransportInterface):
    """A transport nothing ever arrives on: its worker waits until stopped."""

    def __init__(self) -> None:
        self._queue: asyncio.Queue[Envelope] = asyncio.Queue()

    @override
    async def send(self, envelope: Envelope) -> Envelope:
        return envelope

    @override
    async def get(self) -> AsyncIterator[Envelope]:
        while True:
            yield await self._queue.get()

    @override
    async def ack(self, envelope: Envelope) -> None: ...

    @override
    async def reject(self, envelope: Envelope) -> None: ...


@final
class IdleTransportFactory(TransportFactoryInterface):
    @override
    def supports(self, dsn: Dsn) -> bool:
        return dsn.scheme == "idle"

    @override
    def create(self, group: Mapping[str, TransportConfig]) -> Mapping[str, TransportInterface]:
        return {name: IdleTransport() for name in group}


CONFIG = MessageBusConfig(
    transports={
        "jobs": TransportConfig("in-memory://"),
        "idle": TransportConfig("idle://"),
    },
    routing={ConsumeJob: "jobs"},
)


@pytest.fixture
def handled() -> list[UUID]:
    return []


@pytest.fixture
def factories() -> list[TransportFactoryInterface]:
    return [InMemoryTransportFactory(), IdleTransportFactory()]


@pytest.fixture
def console() -> CommandTester:
    return CommandTester(Application(catch_exceptions=False), "messenger:consume")


@pytest.fixture
def tester(
    console: CommandTester, handled: list[UUID], factories: list[TransportFactoryInterface]
) -> Iterator[CommandTester]:
    handlers = HandlersLocator()

    @as_message_handler(ConsumeJob, handlers)
    async def run(message: ConsumeJob) -> None:
        handled.append(message.job_id)

    ConsumeMessagesCommand.use_workers(WorkerFactory(CONFIG, factories, handlers))
    yield console
    ConsumeMessagesCommand.use_workers(None)


async def test_it_handles_what_the_named_transport_holds(
    tester: CommandTester,
    handled: list[UUID],
    factories: list[TransportFactoryInterface],
) -> None:
    bus = MessageBusFactory(CONFIG, factories).bus()
    first, second = uuid4(), uuid4()
    _ = await bus.dispatch(ConsumeJob(first))
    _ = await bus.dispatch(ConsumeJob(second))

    code = await tester.execute(["jobs"])

    assert code == ExitCode.SUCCESS
    assert handled == [first, second]
    assert "Consuming messages from jobs." in tester.display


async def test_it_refuses_to_run_without_a_transport(tester: CommandTester) -> None:
    code = await tester.execute([])

    assert code == ExitCode.INVALID
    assert "Name at least one transport to consume." in tester.display


async def test_it_reports_a_transport_the_configuration_does_not_define(
    tester: CommandTester,
) -> None:
    code = await tester.execute(["missing"])

    assert code == ExitCode.FAILURE
    assert "missing" in tester.display


async def test_it_stops_waiting_once_the_time_limit_passes(tester: CommandTester) -> None:
    async with asyncio.timeout(5):
        code = await tester.execute(["idle", "--time-limit", "0.05"])

    assert code == ExitCode.SUCCESS


async def test_it_refuses_a_time_limit_that_is_not_positive(tester: CommandTester) -> None:
    code = await tester.execute(["idle", "--time-limit", "0"])

    assert code == ExitCode.INVALID


@pytest.mark.skipif(sys.platform == "win32", reason="the event loop cannot handle signals")
async def test_sigterm_stops_the_worker_and_is_handed_back(tester: CommandTester) -> None:
    loop = asyncio.get_running_loop()
    _ = loop.call_later(0.05, os.kill, os.getpid(), signal.SIGTERM)

    async with asyncio.timeout(5):
        code = await tester.execute(["idle"])

    assert code == ExitCode.SUCCESS
    assert not loop.remove_signal_handler(signal.SIGTERM)


async def test_it_asks_for_a_factory_when_neither_a_container_nor_one_is_given(
    console: CommandTester,
) -> None:
    code = await console.execute(["jobs"])

    assert code == ExitCode.FAILURE
    assert "No worker factory" in console.display
