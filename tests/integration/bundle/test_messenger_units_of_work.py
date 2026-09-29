"""With a kernel, a message is a unit of work, and a chain entry can give middleware arguments."""

from __future__ import annotations

import pytest
from xtr_dependency_injection import Kernel

from tests.fixtures.app_units.messages import Record
from tests.fixtures.app_units.services import Journal
from xtr_messenger import Envelope, MessageBusInterface, MiddlewareInterface

pytestmark = pytest.mark.anyio

APP = "tests.fixtures.app_units"


async def test_every_handler_of_a_message_shares_one_session_released_with_the_message() -> None:
    async with await Kernel(APP, env="test").boot() as booted:
        bus = await booted.container.get(MessageBusInterface)
        journal = await booted.container.get(Journal)

        _ = await bus.dispatch(Envelope(Record("a")))
        _ = await bus.dispatch(Envelope(Record("b")))

    seen = dict(journal.sessions)
    assert seen["first:a"] is seen["second:a"] is seen["nested:a"]
    assert seen["first:b"] is seen["second:b"] is seen["nested:b"]
    assert seen["first:a"] is not seen["first:b"]
    assert all(session.closed for session in seen.values())


async def test_a_chain_entry_with_arguments_is_a_middleware_of_its_own() -> None:
    async with await Kernel(APP, env="test").boot() as booted:
        bus = await booted.container.get(MessageBusInterface)
        journal = await booted.container.get(Journal)

        _ = await bus.dispatch(Envelope(Record("a")))

        bare = await booted.container.get(MiddlewareInterface, "labelled")
        ordered = await booted.container.get(MiddlewareInterface, "labelled#0")
        assert bare is not ordered

    # The nested dispatch runs the chain again, inside the same unit of work.
    assert journal.middleware[:3] == [("plain", 1), ("first", 1), ("plain", 2)]
