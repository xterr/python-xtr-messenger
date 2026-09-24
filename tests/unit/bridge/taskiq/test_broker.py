"""The publish-side connection lifecycle: open once, per broker, safely."""

from __future__ import annotations

import asyncio
import gc
from typing import TYPE_CHECKING, final

import pytest
from taskiq import InMemoryBroker
from typing_extensions import override

from message_bus.bridge.taskiq.broker import ensure_started, forget_started

if TYPE_CHECKING:
    from collections.abc import Iterator

pytestmark = pytest.mark.anyio


@final
class CountingBroker(InMemoryBroker):
    """An in-memory broker that counts how often it is started.

    Subclassed rather than patched: overriding ``startup`` keeps the count
    without reaching into taskiq's internals.
    """

    def __init__(self) -> None:
        super().__init__()
        self.startups = 0

    @override
    async def startup(self) -> None:
        self.startups += 1
        await super().startup()


@pytest.fixture
def broker() -> Iterator[CountingBroker]:
    made = CountingBroker()
    yield made
    forget_started(made)


async def test_a_broker_is_started_only_once(broker: CountingBroker) -> None:
    await ensure_started(broker)
    await ensure_started(broker)

    assert broker.startups == 1


async def test_a_worker_process_is_not_started_again(broker: CountingBroker) -> None:
    """In a worker the receiver already opened the broker; starting it again
    re-fires the startup events and duplicates whatever they set up."""
    broker.is_worker_process = True

    await ensure_started(broker)

    assert broker.startups == 0


async def test_concurrent_first_publishes_start_it_once(broker: CountingBroker) -> None:
    _ = await asyncio.gather(
        ensure_started(broker),
        ensure_started(broker),
        ensure_started(broker),
    )

    assert broker.startups == 1


async def test_two_brokers_are_each_started(broker: CountingBroker) -> None:
    """The flag is per broker instance, so an app with two brokers opens each."""
    other = CountingBroker()

    await ensure_started(broker)
    await ensure_started(other)

    assert (broker.startups, other.startups) == (1, 1)
    forget_started(other)


async def test_forgetting_a_broker_lets_it_start_again(broker: CountingBroker) -> None:
    await ensure_started(broker)
    forget_started(broker)

    await ensure_started(broker)

    assert broker.startups == 2


async def test_the_lock_double_checks_before_starting() -> None:
    """Two first publishes race for the lock: the one that waits finds the
    broker already started inside the lock and does not open it again."""
    release = asyncio.Event()
    entered = asyncio.Event()

    @final
    class GatedBroker(InMemoryBroker):
        def __init__(self) -> None:
            super().__init__()
            self.startups = 0

        @override
        async def startup(self) -> None:
            entered.set()
            _ = await release.wait()
            self.startups += 1
            await super().startup()

    broker = GatedBroker()
    first = asyncio.create_task(ensure_started(broker))
    _ = await entered.wait()

    second = asyncio.create_task(ensure_started(broker))
    await asyncio.sleep(0)

    release.set()
    _ = await asyncio.gather(first, second)

    assert broker.startups == 1
    forget_started(broker)


async def test_a_collected_broker_does_not_hand_its_identity_to_the_next() -> None:
    """H1. Started state was keyed by ``id()``, which is reused after
    collection, so a fresh broker could inherit "already started" and never
    open. A weak set keyed by the broker itself dies with it instead.
    """
    first = CountingBroker()
    await ensure_started(first)
    assert first.startups == 1

    del first
    _ = gc.collect()

    second = CountingBroker()
    await ensure_started(second)

    assert second.startups == 1
    forget_started(second)
