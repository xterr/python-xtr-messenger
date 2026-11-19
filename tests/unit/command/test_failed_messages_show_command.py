from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING

import pytest
from xtr_console import ExitCode

from tests.unit.command.conftest import InvoiceSent, OrderPlaced, failed

if TYPE_CHECKING:
    from tests.unit.command.conftest import World

pytestmark = pytest.mark.anyio


async def test_it_lists_the_failed_messages(world: World) -> None:
    _ = world.failed.keep(failed(OrderPlaced("o-1")))
    tester = world.tester("messenger:failed:show")

    assert await tester.execute() == ExitCode.SUCCESS
    assert "There is 1 message pending in the failure transport." in tester.display
    assert "2026-01-01 12:00:00" in tester.display
    assert "boom" in tester.display


async def test_it_selects_by_when_a_message_failed(world: World) -> None:
    _ = world.failed.keep(failed(OrderPlaced("o-1")))
    _ = world.failed.keep(failed(InvoiceSent("i-1"), datetime(2026, 3, 1, tzinfo=UTC)))
    tester = world.tester("messenger:failed:show")

    code = await tester.execute(["--failed-after", "2026-02-01T00:00:00+00:00"])

    assert code == ExitCode.SUCCESS
    assert "InvoiceSent" in tester.display
    assert "OrderPlaced" not in tester.display


async def test_it_shows_one_failed_message(world: World) -> None:
    message_id = world.failed.keep(failed(OrderPlaced("o-1")))
    tester = world.tester("messenger:failed:show")

    assert await tester.execute([message_id]) == ExitCode.SUCCESS
    display = tester.display
    assert "Failed Message Details" in display
    assert "RuntimeError" in display
    assert f"messenger:failed:retry {message_id} --transport=failed" in display


async def test_an_id_cannot_be_combined_with_a_filter(world: World) -> None:
    tester = world.tester("messenger:failed:show")

    assert await tester.execute(["1", "--class-filter", "x"]) == ExitCode.FAILURE
    assert "cannot be combined" in tester.display


async def test_a_date_that_does_not_read_is_refused(world: World) -> None:
    tester = world.tester("messenger:failed:show")

    assert await tester.execute(["--failed-after", "whenever"]) == ExitCode.FAILURE
    assert "is not a date" in tester.display


async def test_a_transport_that_is_not_a_failure_transport_is_refused(world: World) -> None:
    tester = world.tester("messenger:failed:show")

    assert await tester.execute(["--transport", "orders"]) == ExitCode.FAILURE
    assert "unknown transport(s): orders" in tester.display
