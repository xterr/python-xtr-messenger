from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from xtr_console import ExitCode

from tests.unit.command.conftest import OrderPlaced, failed
from xtr_messenger import ReceivedStamp, SentToFailureTransportStamp, TransportMessageIdStamp

if TYPE_CHECKING:
    from tests.unit.command.conftest import World

pytestmark = pytest.mark.anyio


async def test_a_retried_message_is_handled_and_acknowledged(world: World) -> None:
    message_id = world.failed.keep(failed(OrderPlaced("o-1")))
    tester = world.tester("messenger:failed:retry")

    assert await tester.execute([message_id, "--force"]) == ExitCode.SUCCESS
    assert world.handled == ["o-1"]
    assert world.failed.kept == {}
    assert "All done!" in tester.display


async def test_a_message_failing_again_goes_back_to_its_failure_transport(world: World) -> None:
    message_id = world.failed.keep(failed(OrderPlaced("o-1")))
    world.fail_handling = True
    tester = world.tester("messenger:failed:retry")

    assert await tester.execute([message_id, "--force"]) == ExitCode.SUCCESS
    [rejected] = world.failed.rejected
    assert rejected.last(ReceivedStamp) == ReceivedStamp("failed")


async def test_redispatch_sends_the_message_again_without_its_failure(world: World) -> None:
    message_id = world.failed.keep(failed(OrderPlaced("o-1")))
    tester = world.tester("messenger:failed:retry")

    assert await tester.execute([message_id, "--force", "--redispatch"]) == ExitCode.SUCCESS
    [sent] = world.bus.dispatched
    assert sent.message == OrderPlaced("o-1")
    assert sent.last(SentToFailureTransportStamp) is None
    assert sent.last(TransportMessageIdStamp) is None
    assert world.failed.kept == {}
    assert world.handled == []


async def test_interactively_each_message_is_offered_once(world: World) -> None:
    _ = world.failed.keep(failed(OrderPlaced("o-1")))
    _ = world.failed.keep(failed(OrderPlaced("o-2")))
    _ = world.failed.keep(failed(OrderPlaced("o-3")))
    tester = world.tester("messenger:failed:retry")

    code = await tester.execute([], inputs=["retry", "skip", "delete"])

    assert code == ExitCode.SUCCESS
    assert world.handled == ["o-1"]
    assert [e.message for e in world.failed.kept.values()] == [OrderPlaced("o-2")]


async def test_without_ids_it_refuses_to_run_non_interactively(world: World) -> None:
    tester = world.tester("messenger:failed:retry")

    assert await tester.execute([], interactive=False) == ExitCode.FAILURE
    assert "non-interactive" in tester.display


async def test_a_message_it_does_not_find_is_reported(world: World) -> None:
    tester = world.tester("messenger:failed:retry")

    assert await tester.execute(["9", "--force"]) == ExitCode.FAILURE
    assert 'The message "9" was not found.' in tester.display
