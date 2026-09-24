"""The full set of settings an AMQP transport reads from its configuration."""

from __future__ import annotations

import pytest

from xtr_messenger import InvalidTransportOptionError
from xtr_messenger.bridge.amqp.amqp_options import AMQP_OPTIONS, AmqpOptions
from xtr_messenger.bridge.amqp.reliability import Reliability


def test_the_defaults() -> None:
    options = AmqpOptions()

    assert options.prefetch_count == 10
    assert options.auto_setup is True
    assert options.reliability == Reliability()


def test_the_option_list_names_every_group() -> None:
    for setting in (
        "prefetch_count",
        "auto_setup",
        "max_attempts",
        "exchange",
        "queue",
        "heartbeat",
    ):
        assert setting in AMQP_OPTIONS


def test_from_settings_composes_every_group() -> None:
    options = AmqpOptions.from_settings(
        {
            "prefetch_count": "50",
            "auto_setup": "false",
            "max_attempts": "10",
            "exchange": "jobs.ex",
            "queue_type": "classic",
            "heartbeat": "30",
        },
    )

    assert options.prefetch_count == 50
    assert options.auto_setup is False
    assert options.reliability.max_attempts == 10
    assert options.exchange.name == "jobs.ex"
    assert options.queue.type == "classic"
    assert options.connection.heartbeat == 30.0


def test_options_given_in_code_are_the_defaults_the_settings_override() -> None:
    """A DSN lives in an environment variable; options let code override it,
    while everything the settings do not mention keeps the coded default."""
    built = AmqpOptions.from_settings(
        {"max_attempts": "7"},
        AmqpOptions(reliability=Reliability(base_delay_seconds=9.0), prefetch_count=99),
    )

    assert built.reliability.max_attempts == 7
    assert built.reliability.base_delay_seconds == 9.0
    assert built.prefetch_count == 99


def test_a_non_numeric_prefetch_count_is_refused() -> None:
    with pytest.raises(InvalidTransportOptionError, match="prefetch_count"):
        _ = AmqpOptions.from_settings({"prefetch_count": "many"})


def test_auto_setup_must_be_a_boolean() -> None:
    with pytest.raises(InvalidTransportOptionError, match="auto_setup"):
        _ = AmqpOptions.from_settings({"auto_setup": "yes"})
