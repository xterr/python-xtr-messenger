"""Quarantining poison deliveries instead of holding them unacknowledged."""

from __future__ import annotations

import logging
from typing import final

import pytest
from taskiq import AckableMessage, InMemoryBroker, TaskiqMessage

from xtr_messenger.bridge.taskiq.poison_message_receiver import PoisonMessageReceiver

pytestmark = pytest.mark.anyio


@final
class Quarantine:
    """Records what was dead-lettered; optionally refuses to."""

    def __init__(self, failure: Exception | None = None) -> None:
        self.letters: list[tuple[bytes, str]] = []
        self._failure: Exception | None = failure

    async def __call__(self, body: bytes, reason: str) -> None:
        if self._failure is not None:
            raise self._failure
        self.letters.append((body, reason))


@final
class Ack:
    """Records whether the delivery was acknowledged."""

    def __init__(self) -> None:
        self.acked: bool = False

    def __call__(self) -> None:
        self.acked = True


@pytest.fixture
def broker() -> InMemoryBroker:
    return InMemoryBroker(await_inplace=True)


def _receiver(broker: InMemoryBroker, quarantine: Quarantine | None) -> PoisonMessageReceiver:
    return PoisonMessageReceiver(
        broker,
        dead_letter=quarantine,
        max_async_tasks=10,
        run_startup=False,
    )


def _unknown_task_body(broker: InMemoryBroker) -> bytes:
    message = TaskiqMessage(
        task_id="t1", task_name="nobody.registered", labels={}, args=[], kwargs={}
    )
    return broker.formatter.dumps(message).message


async def test_an_unparsable_delivery_is_dead_lettered_and_acked(broker: InMemoryBroker) -> None:
    quarantine = Quarantine()
    ack = Ack()

    await _receiver(broker, quarantine).callback(AckableMessage(data=b"\x00not json", ack=ack))

    [(body, reason)] = quarantine.letters
    assert body == b"\x00not json"
    assert "parsed" in reason
    assert ack.acked


async def test_a_delivery_for_an_unregistered_task_is_dead_lettered_and_acked(
    broker: InMemoryBroker,
) -> None:
    quarantine = Quarantine()
    ack = Ack()

    await _receiver(broker, quarantine).callback(
        AckableMessage(data=_unknown_task_body(broker), ack=ack)
    )

    [(_, reason)] = quarantine.letters
    assert "nobody.registered" in reason
    assert ack.acked


async def test_a_failed_quarantine_leaves_the_delivery_unacknowledged(
    broker: InMemoryBroker,
) -> None:
    """Acknowledging a delivery that was not put anywhere would drop it; left
    unsettled, the broker redelivers and quarantining is tried again."""
    ack = Ack()

    with pytest.raises(RuntimeError):
        await _receiver(broker, Quarantine(failure=RuntimeError("closed"))).callback(
            AckableMessage(data=b"\x00not json", ack=ack)
        )

    assert not ack.acked


async def test_without_a_quarantine_a_poison_delivery_is_acked_and_dropped(
    broker: InMemoryBroker,
) -> None:
    """No dead-letter queue configured means exhausted and poison messages
    alike are dropped — but settled, never redelivered forever."""
    ack = Ack()

    await _receiver(broker, None).callback(AckableMessage(data=b"\x00not json", ack=ack))

    assert ack.acked


async def test_the_log_names_the_reason_and_never_the_body(
    broker: InMemoryBroker, caplog: pytest.LogCaptureFixture
) -> None:
    """A body may carry anything a producer put in a message — secrets
    included — so only the reason reaches the log."""
    secret = b"s3cret-payload"
    with caplog.at_level(logging.WARNING, logger="messenger"):
        await _receiver(broker, Quarantine()).callback(AckableMessage(data=secret, ack=Ack()))

    assert any("poison" in record.getMessage() for record in caplog.records)
    assert all(secret.decode() not in record.getMessage() for record in caplog.records)


async def test_a_delivery_for_a_registered_task_goes_through(broker: InMemoryBroker) -> None:
    """Screening must not swallow healthy deliveries: a parsable body naming
    a registered task reaches the loop's own handling."""
    calls: list[int] = []

    async def fire() -> None:
        calls.append(1)

    _ = broker.register_task(fire, task_name="test.poison.fire")
    quarantine = Quarantine()
    message = TaskiqMessage(
        task_id="t2", task_name="test.poison.fire", labels={}, args=[], kwargs={}
    )
    ack = Ack()

    await _receiver(broker, quarantine).callback(
        AckableMessage(data=broker.formatter.dumps(message).message, ack=ack)
    )

    assert calls == [1]
    assert quarantine.letters == []
