"""Unit tests for :class:`xtr_messenger.middleware.unit_of_work_middleware.UnitOfWorkMiddleware`."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from xtr_dependency_injection import Kernel, current_unit_of_work

from tests.support.fakes import OneStep
from tests.support.messages import ingest_document
from xtr_messenger import Envelope
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
