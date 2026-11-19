from __future__ import annotations

import pytest

from tests.support.fakes import StubReceiver
from tests.support.listing import ListedTransport, ListedTransportFactory
from xtr_messenger import (
    InMemoryTransport,
    InMemoryTransportFactory,
    MessageBusConfig,
    TransportConfig,
    TransportLocator,
    UnknownTransportError,
)


def _locator(**receivers: StubReceiver) -> tuple[TransportLocator, ListedTransport]:
    listed = ListedTransport()
    config = MessageBusConfig(
        transports={"orders": TransportConfig("listed://"), "mem": TransportConfig("in-memory://")}
    )
    factories = [ListedTransportFactory({"orders": listed}), InMemoryTransportFactory()]
    return TransportLocator(config, factories, receivers=receivers), listed


def test_it_hands_over_each_configured_transport_built_once() -> None:
    locator, listed = _locator()

    assert locator.get("orders") is listed
    assert isinstance(locator.get("mem"), InMemoryTransport)
    assert locator.get("mem") is locator.get("mem")


def test_it_names_the_configured_transports_then_the_registered_receivers() -> None:
    ticks = StubReceiver()
    locator, _ = _locator(ticks=ticks, orders=StubReceiver())

    assert locator.names() == ("orders", "mem", "ticks")
    assert locator.get("ticks") is ticks
    assert locator.has("ticks")


def test_a_configured_transport_wins_over_a_receiver_of_the_same_name() -> None:
    locator, listed = _locator(orders=StubReceiver())

    assert locator.get("orders") is listed


def test_an_unknown_name_is_refused_naming_what_there_is() -> None:
    locator, _ = _locator()

    assert not locator.has("missing")
    with pytest.raises(UnknownTransportError) as raised:
        _ = locator.get("missing")
    assert raised.value.known == ("orders", "mem")
