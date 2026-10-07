"""The publish-side connection lifecycle: open once, per broker, per owner."""

from __future__ import annotations

import asyncio
import gc
from typing import final

import pytest
from taskiq import InMemoryBroker
from typing_extensions import override

from xtr_messenger.bridge.taskiq.started_brokers import StartedBrokers

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
        self.shutdowns = 0

    @override
    async def startup(self) -> None:
        self.startups += 1
        await super().startup()

    @override
    async def shutdown(self) -> None:
        self.shutdowns += 1
        await super().shutdown()


@pytest.fixture
def broker() -> CountingBroker:
    return CountingBroker()


@pytest.fixture
def publishers() -> StartedBrokers:
    return StartedBrokers()


async def test_a_broker_is_started_only_once(
    publishers: StartedBrokers, broker: CountingBroker
) -> None:
    await publishers.ensure_started(broker)
    await publishers.ensure_started(broker)

    assert broker.startups == 1


async def test_a_worker_process_is_not_started_again(
    publishers: StartedBrokers, broker: CountingBroker
) -> None:
    """In a worker the receiver already opened the broker; starting it again
    re-fires the startup events and duplicates whatever they set up."""
    broker.is_worker_process = True

    await publishers.ensure_started(broker)

    assert broker.startups == 0


async def test_concurrent_first_publishes_start_it_once(
    publishers: StartedBrokers, broker: CountingBroker
) -> None:
    _ = await asyncio.gather(
        publishers.ensure_started(broker),
        publishers.ensure_started(broker),
        publishers.ensure_started(broker),
    )

    assert broker.startups == 1


async def test_two_brokers_are_each_started(
    publishers: StartedBrokers, broker: CountingBroker
) -> None:
    """The record is per broker instance, so an owner with two brokers opens each."""
    other = CountingBroker()

    await publishers.ensure_started(broker)
    await publishers.ensure_started(other)

    assert (broker.startups, other.startups) == (1, 1)


async def test_forgetting_a_broker_lets_it_start_again(
    publishers: StartedBrokers, broker: CountingBroker
) -> None:
    await publishers.ensure_started(broker)
    publishers.forget(broker)

    await publishers.ensure_started(broker)

    assert broker.startups == 2


async def test_the_lock_double_checks_before_starting(publishers: StartedBrokers) -> None:
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
    first = asyncio.create_task(publishers.ensure_started(broker))
    _ = await entered.wait()

    second = asyncio.create_task(publishers.ensure_started(broker))
    await asyncio.sleep(0)

    release.set()
    _ = await asyncio.gather(first, second)

    assert broker.startups == 1


async def test_a_collected_broker_does_not_hand_its_identity_to_the_next(
    publishers: StartedBrokers,
) -> None:
    """H1. Started state was keyed by ``id()``, which is reused after
    collection, so a fresh broker could inherit "already started" and never
    open. A weak set keyed by the broker itself dies with it instead.
    """
    first = CountingBroker()
    await publishers.ensure_started(first)
    assert first.startups == 1

    del first
    _ = gc.collect()

    second = CountingBroker()
    await publishers.ensure_started(second)

    assert second.startups == 1


async def test_close_all_shuts_down_and_forgets_started_brokers(
    publishers: StartedBrokers, broker: CountingBroker
) -> None:
    """A publisher opens its connection on first publish; closing is what an
    application shutdown does to release it again."""
    await publishers.ensure_started(broker)

    await publishers.close_all()

    assert broker.shutdowns == 1
    await publishers.ensure_started(broker)
    assert broker.startups == 2


async def test_closing_one_owners_brokers_leaves_anothers_open(
    publishers: StartedBrokers, broker: CountingBroker
) -> None:
    """M1. One process can run two applications. An owner that closed every
    broker started in the process would drop a connection the other is still
    publishing through."""
    theirs = CountingBroker()
    other_owner = StartedBrokers()
    await publishers.ensure_started(broker)
    await other_owner.ensure_started(theirs)

    await publishers.close_all()

    assert (broker.shutdowns, theirs.shutdowns) == (1, 0)


@final
class FailingBroker(InMemoryBroker):
    """An in-memory broker whose shutdown raises, to test close_all's spread."""

    def __init__(self, label: str) -> None:
        super().__init__()
        self.label = label
        self.shutdowns = 0

    @override
    async def shutdown(self) -> None:
        self.shutdowns += 1
        raise RuntimeError(self.label)


async def test_close_all_tries_every_broker_even_when_one_fails(
    publishers: StartedBrokers, broker: CountingBroker
) -> None:
    """A shutdown that raised once left the brokers behind it open. close_all
    now closes every one before it re-raises."""
    first = FailingBroker("first")
    await publishers.ensure_started(first)
    await publishers.ensure_started(broker)

    with pytest.raises(ExceptionGroup):
        await publishers.close_all()

    assert first.shutdowns == 1
    assert broker.shutdowns == 1


async def test_close_all_raises_an_exception_group_of_the_failures(
    publishers: StartedBrokers,
) -> None:
    """Every shutdown that raised is collected and re-raised together, so no
    failure is swallowed by the one that happens to run first."""
    first = FailingBroker("first")
    second = FailingBroker("second")
    await publishers.ensure_started(first)
    await publishers.ensure_started(second)

    with pytest.raises(ExceptionGroup) as excinfo:
        await publishers.close_all()

    raised = {str(error) for error in excinfo.value.exceptions}
    assert raised == {"first", "second"}


async def test_close_all_forgets_even_the_brokers_that_failed_to_close(
    publishers: StartedBrokers,
) -> None:
    """A broker left recorded after a failed close would be shut down a second
    time by the next close_all. Each is forgotten as it is tried."""
    first = FailingBroker("first")
    await publishers.ensure_started(first)

    with pytest.raises(ExceptionGroup):
        await publishers.close_all()
    await publishers.close_all()

    assert first.shutdowns == 1
