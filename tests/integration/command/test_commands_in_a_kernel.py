"""End-to-end: every messenger command runs from a booted kernel's console."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pytest
from xtr_console import Application, ApplicationTester, ExitCode
from xtr_dependency_injection import Kernel

from xtr_messenger import FailedMessageRepository, TransportLocator

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

pytestmark = pytest.mark.anyio

_COMMANDS = (
    "messenger:consume",
    "messenger:setup-transports",
    "messenger:stats",
    "messenger:show",
    "debug:messenger",
    "messenger:failed:show",
    "messenger:failed:remove",
    "messenger:failed:retry",
)


@pytest.fixture
async def tester() -> AsyncIterator[ApplicationTester]:
    booted = await Kernel("tests.fixtures.app_commands", env="test").boot()
    try:
        locator = await booted.container.get(TransportLocator)
        repository = await booted.container.get(FailedMessageRepository)
        assert locator.names() == ("jobs", "failed")
        assert repository.global_transport_name == "failed"
        yield ApplicationTester(await booted.container.get(Application), width=200)
    finally:
        await booted.shutdown()


async def test_every_command_is_listed(tester: ApplicationTester) -> None:
    assert await tester.execute(["list"]) == ExitCode.SUCCESS
    for command in _COMMANDS:
        assert command in tester.display


async def test_debug_shows_the_routing_and_failure_transport(tester: ApplicationTester) -> None:
    assert await tester.execute(["debug:messenger"]) == ExitCode.SUCCESS
    assert "tests.fixtures.app_commands.messages.Shipped" in tester.display
    assert "record_shipment" in tester.display
    assert "failed" in tester.display


async def test_the_transport_commands_say_what_in_memory_cannot_do(
    tester: ApplicationTester,
) -> None:
    assert await tester.execute(["messenger:setup-transports"]) == ExitCode.SUCCESS
    assert "does not support setup" in tester.display
    assert await tester.execute(["messenger:stats", "--format", "json"]) == ExitCode.SUCCESS
    assert json.loads(tester.display)["uncountable_transports"] == ["jobs", "failed"]
    assert await tester.execute(["messenger:show", "jobs"]) == ExitCode.FAILURE
    assert "cannot list messages" in tester.display
    assert await tester.execute(["messenger:failed:show"]) == ExitCode.FAILURE
    assert "cannot list messages" in tester.display
