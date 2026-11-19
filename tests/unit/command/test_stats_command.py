from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pytest
from xtr_console import ExitCode

from tests.unit.command.conftest import OrderPlaced
from xtr_messenger import Envelope

if TYPE_CHECKING:
    from tests.unit.command.conftest import World

pytestmark = pytest.mark.anyio


async def test_it_counts_each_transport_and_names_those_it_cannot(world: World) -> None:
    _ = world.orders.keep(Envelope(OrderPlaced("o-1")))
    tester = world.tester("messenger:stats")

    code = await tester.execute()

    assert code == ExitCode.SUCCESS
    assert "orders" in tester.display
    assert '"mem"' in tester.display


async def test_it_writes_json(world: World) -> None:
    _ = world.orders.keep(Envelope(OrderPlaced("o-1")))
    tester = world.tester("messenger:stats")

    code = await tester.execute(["orders", "mem", "--format", "json"])

    assert code == ExitCode.SUCCESS
    assert json.loads(tester.display) == {
        "transports": {"orders": {"count": 1}},
        "uncountable_transports": ["mem"],
    }


async def test_it_warns_of_a_transport_that_does_not_exist(world: World) -> None:
    tester = world.tester("messenger:stats")

    assert await tester.execute(["missing"]) == ExitCode.SUCCESS
    assert 'The "missing" transport does not exist.' in tester.display
