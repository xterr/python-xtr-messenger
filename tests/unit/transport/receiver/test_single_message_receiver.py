from __future__ import annotations

from typing import TYPE_CHECKING, override

import pytest

from tests.support.fakes import StubReceiver
from xtr_messenger import (
    Envelope,
    ReceiverInterface,
    RetryingReceiverInterface,
    SingleMessageReceiver,
)

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

pytestmark = pytest.mark.anyio


class _Retrying(ReceiverInterface, RetryingReceiverInterface):
    """Retries everything; receives nothing."""

    @override
    async def get(self) -> AsyncIterator[Envelope]:
        for envelope in ():
            yield envelope

    @override
    async def ack(self, envelope: Envelope) -> None:
        del envelope

    @override
    async def reject(self, envelope: Envelope) -> None:
        del envelope

    @override
    def will_retry(self, envelope: Envelope) -> bool:
        return True


async def test_it_yields_its_one_envelope_once() -> None:
    envelope = Envelope("one")
    receiver = SingleMessageReceiver(StubReceiver(), envelope)

    assert [e async for e in receiver.get()] == [envelope]
    assert [e async for e in receiver.get()] == []


async def test_it_settles_through_the_receiver_holding_it() -> None:
    held = StubReceiver()
    receiver = SingleMessageReceiver(held, Envelope("one"))

    await receiver.ack(Envelope("one"))
    await receiver.reject(Envelope("two"))

    assert held.acked == [Envelope("one")]
    assert held.rejected == [Envelope("two")]


def test_it_retries_as_the_receiver_holding_it_does() -> None:
    assert SingleMessageReceiver(_Retrying(), Envelope("x")).will_retry(Envelope("x"))
    assert not SingleMessageReceiver(StubReceiver(), Envelope("x")).will_retry(Envelope("x"))
