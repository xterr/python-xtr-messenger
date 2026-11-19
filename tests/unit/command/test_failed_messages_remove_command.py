from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from xtr_console import ExitCode

from tests.unit.command.conftest import InvoiceSent, OrderPlaced, failed

if TYPE_CHECKING:
    from tests.unit.command.conftest import World

pytestmark = pytest.mark.anyio


async def test_it_removes_a_message_by_id_once_forced(world: World) -> None:
    message_id = world.failed.keep(failed(OrderPlaced("o-1")))
    tester = world.tester("messenger:failed:remove")

    assert await tester.execute([message_id, "--force"]) == ExitCode.SUCCESS
    assert world.failed.kept == {}
    assert not world.failed.rejected


async def test_it_keeps_a_message_when_removing_is_declined(world: World) -> None:
    message_id = world.failed.keep(failed(OrderPlaced("o-1")))
    tester = world.tester("messenger:failed:remove")

    assert await tester.execute([message_id], inputs=["n"]) == ExitCode.SUCCESS
    assert message_id in world.failed.kept


async def test_it_removes_every_message(world: World) -> None:
    _ = world.failed.keep(failed(OrderPlaced("o-1")))
    _ = world.failed.keep(failed(OrderPlaced("o-2")))
    tester = world.tester("messenger:failed:remove")

    assert await tester.execute(["--all", "--force"]) == ExitCode.SUCCESS
    assert world.failed.kept == {}
    assert "2 messages were removed." in tester.display


async def test_it_removes_what_a_filter_selects_once_confirmed(world: World) -> None:
    _ = world.failed.keep(failed(OrderPlaced("o-1")))
    kept = world.failed.keep(failed(InvoiceSent("i-1")))
    tester = world.tester("messenger:failed:remove")

    code = await tester.execute(
        ["--class-filter", "tests.unit.command.conftest.OrderPlaced"], inputs=["y"]
    )

    assert code == ExitCode.SUCCESS
    assert list(world.failed.kept) == [kept]


async def test_it_needs_an_id_or_all(world: World) -> None:
    tester = world.tester("messenger:failed:remove")

    assert await tester.execute([]) == ExitCode.FAILURE
    assert await tester.execute(["1", "--all"]) == ExitCode.FAILURE
    assert await tester.execute(["--all", "--class-filter", "x"]) == ExitCode.FAILURE
