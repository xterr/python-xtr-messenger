from __future__ import annotations

from typing import TYPE_CHECKING, final

import pytest
from typing_extensions import override

from message_bus import (
    Dsn,
    TransportConfig,
    TransportFactory,
    TransportFactoryInterface,
    UnsupportedDsnError,
    WorkerProvidingInterface,
)
from message_bus.transport.in_memory import InMemoryTransport

if TYPE_CHECKING:
    from collections.abc import Mapping

    from message_bus import MessageBusInterface, WorkerInterface
    from message_bus.transport.sender import SenderInterface


@final
class PigeonFactory(TransportFactoryInterface):
    """A transport factory published from outside the library."""

    def __init__(self) -> None:
        self.built = 0

    @override
    def supports(self, dsn: Dsn) -> bool:
        return dsn.scheme == "carrier-pigeon"

    @override
    def create(self, group: Mapping[str, TransportConfig]) -> Mapping[str, SenderInterface]:
        self.built += 1
        return dict.fromkeys(group, InMemoryTransport())


@final
class WorkerBringingFactory(TransportFactoryInterface, WorkerProvidingInterface):
    """An adapter whose broker owns the consume loop, so it must be seen as itself."""

    @override
    def supports(self, dsn: Dsn) -> bool:
        return dsn.scheme == "own"

    @override
    def create(self, group: Mapping[str, TransportConfig]) -> Mapping[str, SenderInterface]:
        return dict.fromkeys(group, InMemoryTransport())

    @override
    def worker(
        self, group: Mapping[str, TransportConfig], bus: MessageBusInterface
    ) -> WorkerInterface:
        raise NotImplementedError


def a_group(dsn: str) -> dict[str, TransportConfig]:
    return {"t": TransportConfig(dsn)}


def test_the_composite_is_itself_a_transport_factory() -> None:
    """So a bus takes one collaborator, not a list plus the rules for it."""
    assert isinstance(TransportFactory(), TransportFactoryInterface)


def test_it_discovers_by_scheme_when_given_no_factories() -> None:
    composite = TransportFactory()

    assert composite.supports(Dsn.parse("sync://"))
    assert composite.supports(Dsn.parse("amqp://h:5672/"))
    assert not composite.supports(Dsn.parse("carrier-pigeon://"))


def test_an_explicit_list_replaces_discovery_entirely() -> None:
    composite = TransportFactory([PigeonFactory()])

    assert composite.supports(Dsn.parse("carrier-pigeon://"))
    assert not composite.supports(Dsn.parse("sync://"))


def test_an_empty_explicit_list_discovers_nothing() -> None:
    composite = TransportFactory([])

    assert not composite.supports(Dsn.parse("sync://"))
    with pytest.raises(UnsupportedDsnError):
        _ = composite.create(a_group("sync://"))


def test_it_delegates_to_the_factory_that_recognises_the_dsn() -> None:
    pigeons = PigeonFactory()
    composite = TransportFactory([pigeons])

    _ = composite.create(a_group("carrier-pigeon://"))

    assert pigeons.built == 1


def test_the_first_matching_factory_wins() -> None:
    first, second = PigeonFactory(), PigeonFactory()
    composite = TransportFactory([first, second])

    _ = composite.create(a_group("carrier-pigeon://"))

    assert first.built == 1
    assert second.built == 0


def test_an_unrecognised_scheme_names_the_transport_that_carried_it() -> None:
    composite = TransportFactory([PigeonFactory()])

    with pytest.raises(UnsupportedDsnError) as excinfo:
        _ = composite.create(a_group("sync://"))

    assert excinfo.value.transport_name == "t"


def test_composites_nest_because_one_is_just_another_factory() -> None:
    """WorkerFactory hands its composite to the bus this way."""
    pigeons = PigeonFactory()
    nested = TransportFactory([TransportFactory([pigeons])])

    _ = nested.create(a_group("carrier-pigeon://"))

    assert pigeons.built == 1


def test_serving_sees_through_a_nested_composite_to_the_real_factory() -> None:
    """A WorkerProvidingInterface adapter behind a nested composite must be
    returned as itself, or the worker it brings would be hidden."""
    adapter = WorkerBringingFactory()
    nested = TransportFactory([TransportFactory([adapter])])

    assert nested.serving(a_group("own://")) is adapter


def test_serving_discovers_and_reuses_one_factory_per_scheme() -> None:
    """A factory holds what it builds, so discovering a fresh one per call
    would hand a worker and the bus two objects for the same server."""
    composite = TransportFactory()
    group = a_group("sync://")

    first = composite.serving(group)
    second = composite.serving(group)

    assert first is second


def test_serving_raises_when_no_discovered_factory_matches() -> None:
    composite = TransportFactory()

    with pytest.raises(UnsupportedDsnError, match="carrier-pigeon"):
        _ = composite.serving(a_group("carrier-pigeon://"))
