"""``messenger:consume`` built by a container, and the core left without xtr-console."""

from __future__ import annotations

import importlib
import subprocess
import sys
from dataclasses import dataclass
from typing import final
from uuid import UUID, uuid4

import pytest
import wireup
from wireup import Injected, injectable
from xtr_console import Application, ApplicationTester, ExitCode
from xtr_console.integration import wireup as console

from xtr_messenger import (
    HandlersLocator,
    InMemoryTransportFactory,
    MessageBusConfig,
    MessageBusInterface,
    TransportConfig,
    as_message,
    as_message_handler,
)
from xtr_messenger.integration import wireup as messenger

pytestmark = pytest.mark.anyio

_ = importlib.import_module("xtr_messenger.command")  # importing declares the command

HANDLERS = HandlersLocator()


@as_message(name="test.integration.command.consume.job.v1")
@dataclass(frozen=True, slots=True)
class ConsumeJob:
    job_id: UUID


@injectable
@final
class Ledger:
    def __init__(self) -> None:
        self.done: list[UUID] = []


@as_message_handler(ConsumeJob, HANDLERS)
async def record(message: ConsumeJob, ledger: Injected[Ledger]) -> None:
    ledger.done.append(message.job_id)


CONFIG = MessageBusConfig(
    transports={"jobs": TransportConfig("in-memory://")}, routing={ConsumeJob: "jobs"}
)


async def test_the_container_builds_the_command_with_its_handlers_wired() -> None:
    container = wireup.create_async_container(
        injectables=[
            Ledger,
            *messenger.injectables(
                CONFIG, factories=[InMemoryTransportFactory()], handlers=HANDLERS
            ),
            *console.injectables(Application(catch_exceptions=False)),
        ],
    )
    try:
        bus = await container.get(MessageBusInterface)
        job = uuid4()
        _ = await bus.dispatch(ConsumeJob(job))

        tester = ApplicationTester(await container.get(Application))
        code = await tester.execute(["messenger:consume", "jobs"])

        assert code == ExitCode.SUCCESS
        assert (await container.get(Ledger)).done == [job]
    finally:
        await container.close()


def test_the_core_and_the_wireup_integration_never_import_the_console() -> None:
    code = (
        "import sys\n"
        "import xtr_messenger\n"
        "import xtr_messenger.integration.wireup\n"
        "assert 'xtr_console' not in sys.modules, 'xtr_console imported without the command'\n"
        "assert 'cyclopts' not in sys.modules, 'cyclopts imported without the command'\n"
    )

    result = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, check=False
    )

    assert result.returncode == 0, result.stderr
