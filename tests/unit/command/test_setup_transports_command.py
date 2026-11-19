from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from xtr_console import ExitCode

if TYPE_CHECKING:
    from tests.unit.command.conftest import World

pytestmark = pytest.mark.anyio


async def test_it_sets_up_every_transport_that_can_be(world: World) -> None:
    tester = world.tester("messenger:setup-transports")

    code = await tester.execute()

    assert code == ExitCode.SUCCESS
    assert world.orders.set_up
    assert world.failed.set_up
    assert 'The "orders" transport was set up successfully.' in tester.display
    assert 'The "mem" transport does not support setup.' in tester.display


async def test_it_sets_up_only_the_transport_named(world: World) -> None:
    code = await world.tester("messenger:setup-transports").execute(["orders"])

    assert code == ExitCode.SUCCESS
    assert world.orders.set_up
    assert not world.failed.set_up


async def test_it_refuses_a_transport_that_does_not_exist(world: World) -> None:
    tester = world.tester("messenger:setup-transports")

    assert await tester.execute(["missing"]) == ExitCode.FAILURE
    assert 'The "missing" transport does not exist.' in tester.display
