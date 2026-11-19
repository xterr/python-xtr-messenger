from __future__ import annotations

import pytest

from xtr_messenger import MessageBusConfig, TransportConfig, UnknownTransportError


def _config(default: str | None = None, **failures: str | None) -> MessageBusConfig:
    return MessageBusConfig(
        transports={
            "orders": TransportConfig("sync://", failure_transport=failures.get("orders")),
            "invoices": TransportConfig("sync://", failure_transport=failures.get("invoices")),
            "failed": TransportConfig("sync://"),
        },
        failure_transport=default,
    )


def test_the_failure_transports_are_the_default_then_each_transports_own() -> None:
    config = _config("failed", invoices="orders")

    assert config.failure_transport_names() == ("failed", "orders")
    assert config.failure_transport_of("orders") == "failed"
    assert config.failure_transport_of("invoices") == "orders"


def test_a_failure_transport_has_none_of_its_own() -> None:
    assert _config("failed").failure_transport_of("failed") is None
    assert _config().failure_transport_of("orders") is None
    assert _config().failure_transport_names() == ()


def test_the_default_failure_transport_must_be_configured() -> None:
    with pytest.raises(UnknownTransportError, match="missing"):
        _ = _config("missing")


def test_a_transports_failure_transport_must_be_configured() -> None:
    with pytest.raises(UnknownTransportError, match="missing"):
        _ = _config(orders="missing")
