from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from xtr_console import ExitCode

if TYPE_CHECKING:
    from tests.unit.command.conftest import World

pytestmark = pytest.mark.anyio


async def test_it_lists_handlers_routes_and_failure_transports(world: World) -> None:
    tester = world.tester("debug:messenger")

    code = await tester.execute()

    assert code == ExitCode.SUCCESS
    display = tester.display
    assert "tests.unit.command.conftest.OrderPlaced" in display
    assert "An order was placed." in display
    assert "World.tester.<locals>.handle" in display
    assert "InvoiceSent (from as_message)" in display
    assert "not handled" in display


async def test_it_inspects_one_message_by_its_declared_name(world: World) -> None:
    tester = world.tester("debug:messenger")

    code = await tester.execute(["--message", "tests.command.invoice_sent.v1"])

    assert code == ExitCode.SUCCESS
    assert "InvoiceSent" in tester.display
    assert "OrderPlaced" not in tester.display.split("Transports")[0]


async def test_it_refuses_a_message_it_does_not_know(world: World) -> None:
    tester = world.tester("debug:messenger")

    assert await tester.execute(["--message", "nope"]) == ExitCode.FAILURE


async def test_it_lists_the_middleware_in_the_order_they_run(world: World) -> None:
    tester = world.tester("debug:messenger")

    assert await tester.execute(["--middleware"]) == ExitCode.SUCCESS
    display = tester.display
    assert display.index("DispatchAfterCurrentBusMiddleware") < display.index(
        "HandleMessageMiddleware"
    )
