"""How the exchange a transport publishes through is declared."""

from __future__ import annotations

import pytest
from aio_pika.abc import ExchangeType

from xtr_messenger import InvalidTransportOptionError
from xtr_messenger.bridge.amqp.exchange_options import ExchangeOptions


def test_the_defaults() -> None:
    options = ExchangeOptions()

    assert options.name is None
    assert options.type == ExchangeType.TOPIC.value
    assert options.durable is True
    assert options.auto_delete is False


def test_from_settings_reads_every_field() -> None:
    options = ExchangeOptions.from_settings(
        {
            "exchange": "jobs.ex",
            "exchange_type": "direct",
            "exchange_durable": "false",
            "exchange_auto_delete": "true",
        },
    )

    assert options.name == "jobs.ex"
    assert options.type == "direct"
    assert options.durable is False
    assert options.auto_delete is True


def test_settings_override_the_given_defaults() -> None:
    built = ExchangeOptions.from_settings(
        {"exchange": "override"},
        ExchangeOptions(name="base", type="fanout"),
    )

    assert built.name == "override"
    assert built.type == "fanout"


def test_an_unknown_exchange_type_names_the_allowed_values() -> None:
    with pytest.raises(InvalidTransportOptionError) as excinfo:
        _ = ExchangeOptions.from_settings({"exchange_type": "nonsense"})

    assert excinfo.value.option == "exchange_type"
    assert "topic" in excinfo.value.expected


def test_no_name_accepts_taskiqs_default_exchange() -> None:
    assert ExchangeOptions().declared(declare=True) is None


def test_declared_builds_the_exchange() -> None:
    exchange = ExchangeOptions(
        name="jobs.ex",
        type="direct",
        durable=False,
        auto_delete=True,
    ).declared(declare=True)

    assert exchange is not None
    assert exchange.name == "jobs.ex"
    assert exchange.type == ExchangeType.DIRECT
    assert exchange.durable is False
    assert exchange.auto_delete is True
    assert exchange.declare is True


def test_declaration_can_be_turned_off() -> None:
    exchange = ExchangeOptions(name="jobs.ex").declared(declare=False)

    assert exchange is not None
    assert exchange.declare is False
