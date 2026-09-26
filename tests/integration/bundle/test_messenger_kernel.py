"""End-to-end: an application listing only MessengerBundle dispatches to a container handler."""

from __future__ import annotations

from typing import TYPE_CHECKING
from uuid import uuid4

import pytest
from xtr_dependency_injection import Kernel

from tests.fixtures.app_messenger.handlers import Ledger
from tests.fixtures.app_messenger.messages import DoWork
from xtr_messenger import Envelope, MessageBusInterface

if TYPE_CHECKING:
    from xtr_service_contracts import ContainerInterface

pytestmark = pytest.mark.anyio


async def test_a_class_handler_receives_a_dispatched_message() -> None:
    kernel = Kernel("tests.fixtures.app_messenger", env="test")
    booted = await kernel.boot()
    try:
        container: ContainerInterface = booted.container
        bus = await container.get(MessageBusInterface)
        job_id = uuid4()

        _ = await bus.dispatch(Envelope(DoWork(job_id)))

        ledger = await container.get(Ledger)
        assert ledger.done == [job_id]
    finally:
        await booted.shutdown()
