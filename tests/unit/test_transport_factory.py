from __future__ import annotations

from typing import TYPE_CHECKING, final

import pytest
from typing_extensions import override

from message_bus import (
    Dsn,
    MessageBusConfig,
    MessageBusFactory,
    TransportConfig,
    TransportFactory,
    TransportFactoryInterface,
    UnsupportedDsnError,
)
from message_bus.transport.in_memory import InMemoryTransport

if TYPE_CHECKING:
    from collections.abc import Mapping

    from message_bus.transport.sender import SenderInterface


@final
class PigeonFactory(TransportFactoryInterface):
    """A transport published from outside the library."""

    def __init__(self) -> None:
        self.built = 0

    @override
    def supports(self, dsn: Dsn) -> bool:
        return dsn.scheme == "carrier-pigeon"

    @override
    def create(self, group: Mapping[str, TransportConfig]) -> Mapping[str, SenderInterface]:
        self.built += 1
        return dict.fromkeys(group, InMemoryTransport())


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


def test_it_delegates_to_the_first_factory_that_recognises_the_dsn() -> None:
    pigeons = PigeonFactory()
    composite = TransportFactory([pigeons])

    _ = composite.create(a_group("carrier-pigeon://"))

    assert pigeons.built == 1


def test_an_unrecognised_scheme_names_the_transport_that_carried_it() -> None:
    composite = TransportFactory([PigeonFactory()])

    with pytest.raises(UnsupportedDsnError, match="t"):
        _ = composite.create(a_group("sync://"))


def test_composites_nest_because_one_is_just_another_factory() -> None:
    """WorkerFactory hands its composite to the bus this way."""
    pigeons = PigeonFactory()
    nested = TransportFactory([TransportFactory([pigeons])])

    _ = nested.create(a_group("carrier-pigeon://"))

    assert pigeons.built == 1


def test_a_bus_built_from_an_explicit_composite_uses_it() -> None:
    pigeons = PigeonFactory()
    config = MessageBusConfig(transports={"odd": TransportConfig("carrier-pigeon://")})

    _ = MessageBusFactory(config, [TransportFactory([pigeons])]).bus()

    assert pigeons.built == 1
