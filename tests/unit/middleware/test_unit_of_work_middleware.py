"""Unit tests for :class:`xtr_messenger.middleware.unit_of_work_middleware.UnitOfWorkMiddleware`."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from xtr_dependency_injection import Kernel, current_unit_of_work, unit_of_work

from tests.fixtures.app_units.services import Session
from tests.support.fakes import OneStep
from tests.support.messages import ingest_document
from xtr_messenger import Envelope, ReceivedStamp
from xtr_messenger.bundle import MessengerBundle
from xtr_messenger.middleware.unit_of_work_middleware import UnitOfWorkMiddleware

if TYPE_CHECKING:
    from xtr_service_contracts import ContainerInterface

    from xtr_messenger import StackInterface

pytestmark = pytest.mark.anyio


class _Seeing:
    """The rest of a chain, noting the unit of work it ran in."""

    def __init__(self) -> None:
        self.units: list[ContainerInterface | None] = []

    async def handle(self, envelope: Envelope, stack: StackInterface, /) -> Envelope:
        del stack
        self.units.append(current_unit_of_work())
        return envelope


async def test_the_rest_of_the_chain_runs_inside_a_unit_of_work() -> None:
    kernel = Kernel(
        MessengerBundle.__module__,
        env="test",
        bundles={MessengerBundle: {"all": True}},
        resources=(),
    )
    seeing = _Seeing()
    async with await kernel.boot() as booted:
        middleware = UnitOfWorkMiddleware(booted.container)

        _ = await middleware.handle(Envelope(ingest_document()), OneStep(seeing))

    assert seeing.units[0] is not None
    assert current_unit_of_work() is None


class _Sessions:
    """The rest of a chain, noting the scoped session of the unit it ran in."""

    def __init__(self) -> None:
        self.sessions: list[Session] = []

    async def handle(self, envelope: Envelope, stack: StackInterface, /) -> Envelope:
        del stack
        unit = current_unit_of_work()
        assert unit is not None
        self.sessions.append(await unit.get(Session))
        return envelope


async def test_a_message_dispatched_inside_a_unit_joins_it() -> None:
    seeing = _Sessions()
    async with await Kernel("tests.fixtures.app_units", env="test").boot() as booted:
        middleware = UnitOfWorkMiddleware(booted.container)
        async with unit_of_work(booted.container) as outer:
            enclosing = await outer.get(Session)
            _ = await middleware.handle(Envelope(ingest_document()), OneStep(seeing))

    assert seeing.sessions == [enclosing]


async def test_a_message_a_worker_received_is_a_unit_of_its_own_inside_another() -> None:
    seeing = _Sessions()
    received = Envelope(ingest_document(), (ReceivedStamp("jobs"),))
    async with await Kernel("tests.fixtures.app_units", env="test").boot() as booted:
        middleware = UnitOfWorkMiddleware(booted.container)
        async with unit_of_work(booted.container) as outer:
            enclosing = await outer.get(Session)
            _ = await middleware.handle(received, OneStep(seeing))

    assert seeing.sessions[0] is not enclosing
    assert seeing.sessions[0].closed
