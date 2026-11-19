from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from xtr_console import ExitCode, Verbosity

from tests.unit.command.conftest import InvoiceSent, OrderPlaced
from xtr_messenger import Envelope

if TYPE_CHECKING:
    from tests.unit.command.conftest import World

pytestmark = pytest.mark.anyio

_ORDER = "tests.unit.command.conftest.OrderPlaced"


async def test_it_lists_the_pending_messages(world: World) -> None:
    _ = world.orders.keep(Envelope(OrderPlaced("o-1")))
    tester = world.tester("messenger:show")

    assert await tester.execute(["orders"]) == ExitCode.SUCCESS
    assert _ORDER in tester.display


async def test_it_filters_by_class_and_counts_by_class(world: World) -> None:
    _ = world.orders.keep(Envelope(OrderPlaced("o-1")))
    _ = world.orders.keep(Envelope(InvoiceSent("i-1")))
    tester = world.tester("messenger:show")

    assert await tester.execute(["orders", "--class-filter", _ORDER]) == ExitCode.SUCCESS
    assert "InvoiceSent" not in tester.display
    assert await tester.execute(["orders", "--stats"]) == ExitCode.SUCCESS
    assert "InvoiceSent" in tester.display


async def test_it_shows_one_message_in_detail(world: World) -> None:
    message_id = world.orders.keep(Envelope(OrderPlaced("o-1")))
    tester = world.tester("messenger:show")

    code = await tester.execute(["orders", message_id], verbosity=Verbosity.VERY_VERBOSE)

    assert code == ExitCode.SUCCESS
    assert "Message Details" in tester.display
    assert "OrderPlaced(order_id='o-1')" in tester.display


async def test_it_reports_a_message_it_does_not_find(world: World) -> None:
    tester = world.tester("messenger:show")

    assert await tester.execute(["orders", "9"]) == ExitCode.FAILURE
    assert 'The message "9" was not found.' in tester.display


async def test_it_refuses_a_transport_that_cannot_be_listed(world: World) -> None:
    tester = world.tester("messenger:show")

    assert await tester.execute(["mem"]) == ExitCode.FAILURE
    assert "cannot list messages" in tester.display


async def test_without_a_transport_it_asks_for_one(world: World) -> None:
    _ = world.orders.keep(Envelope(OrderPlaced("o-1")))
    tester = world.tester("messenger:show")

    assert await tester.execute([], inputs=["orders"]) == ExitCode.SUCCESS
    assert _ORDER in tester.display
    assert await tester.execute([], interactive=False) == ExitCode.INVALID


async def test_an_id_cannot_be_combined_with_stats(world: World) -> None:
    tester = world.tester("messenger:show")

    assert await tester.execute(["orders", "1", "--stats"]) == ExitCode.INVALID
