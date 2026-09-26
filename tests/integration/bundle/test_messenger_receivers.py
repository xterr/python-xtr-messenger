"""End-to-end: a receiver registered in the container is consumable by its alias."""

from __future__ import annotations

import pytest
from xtr_dependency_injection import Kernel

from tests.fixtures.app_receivers.messages import Ticks
from xtr_messenger import MessageBusError, WorkerFactory

pytestmark = pytest.mark.anyio


async def test_a_tagged_receiver_is_consumed_by_its_alias() -> None:
    booted = await Kernel("tests.fixtures.app_receivers", env="test").boot()
    try:
        workers = await booted.container.get(WorkerFactory)

        await workers.worker(["ticks"]).run()

        ticks = await booted.container.get(Ticks)
        assert ticks.seen == [0, 1, 2]
    finally:
        await booted.shutdown()


async def test_a_tagged_receiver_drains_beside_a_configured_transport() -> None:
    booted = await Kernel("tests.fixtures.app_receivers", env="test").boot()
    try:
        workers = await booted.container.get(WorkerFactory)

        await workers.worker(["jobs", "ticks"]).run()

        ticks = await booted.container.get(Ticks)
        assert ticks.seen == [0, 1, 2]
    finally:
        await booted.shutdown()


async def test_two_receivers_under_one_alias_fail_the_build() -> None:
    with pytest.raises(MessageBusError, match="share the alias 'same'"):
        _ = await Kernel("tests.fixtures.app_receivers_clash", env="test").boot()
