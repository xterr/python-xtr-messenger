from __future__ import annotations

import pytest

from xtr_messenger import (
    Dsn,
    SyncTransport,
    SyncTransportFactory,
    TransportConfig,
    UnknownTransportOptionError,
    WorkerProvidingInterface,
)


def test_it_supports_the_sync_scheme() -> None:
    assert SyncTransportFactory().supports(Dsn.parse("sync://"))


def test_it_does_not_support_another_scheme() -> None:
    assert not SyncTransportFactory().supports(Dsn.parse("amqp://host"))


def test_it_builds_a_sync_transport_per_name() -> None:
    built = SyncTransportFactory().create(
        {"a": TransportConfig("sync://"), "b": TransportConfig("sync://")}
    )

    assert set(built) == {"a", "b"}
    assert all(isinstance(transport, SyncTransport) for transport in built.values())


def test_it_refuses_a_dsn_that_carries_any_setting() -> None:
    """A sync transport has nothing to configure, so a setting is a mistake."""
    group = {"sync": TransportConfig("sync://?queue=jobs")}

    with pytest.raises(UnknownTransportOptionError):
        _ = SyncTransportFactory().create(group)


def test_it_does_not_bring_its_own_worker() -> None:
    """Driven by the library's own loop, so it must not claim to provide one."""
    assert not isinstance(SyncTransportFactory(), WorkerProvidingInterface)
