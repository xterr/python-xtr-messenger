from __future__ import annotations

from types import MappingProxyType

import pytest

from xtr_messenger import Dsn, InvalidDsnError, TransportConfig


def test_a_queue_in_the_dsn_is_used_when_no_field_is_given() -> None:
    assert TransportConfig("amqp://rabbit?queue=from_dsn").queue_name == "from_dsn"


def test_an_explicit_queue_field_wins_over_the_dsn() -> None:
    config = TransportConfig("amqp://rabbit?queue=from_dsn", queue="from_field")

    assert config.queue_name == "from_field"


def test_a_transport_with_no_queue_anywhere_reports_none() -> None:
    assert TransportConfig("sync://").queue_name is None


def test_options_override_the_same_setting_in_the_dsn() -> None:
    """A DSN held in an environment variable can be overridden in code."""
    config = TransportConfig("amqp://rabbit?prefetch_count=10", options={"prefetch_count": "50"})

    assert config.settings["prefetch_count"] == "50"


def test_the_queue_field_overrides_a_queue_in_options() -> None:
    config = TransportConfig("amqp://rabbit", queue="from_field", options={"queue": "from_options"})

    assert config.settings["queue"] == "from_field"


def test_settings_merge_the_dsn_query_string_and_options() -> None:
    config = TransportConfig("amqp://rabbit?max_attempts=3", options={"prefetch_count": "50"})

    assert config.settings["max_attempts"] == "3"
    assert config.settings["prefetch_count"] == "50"


def test_settings_are_a_read_only_mapping() -> None:
    settings = TransportConfig("amqp://rabbit?queue=jobs").settings

    assert isinstance(settings, MappingProxyType)


def test_the_dsn_is_parsed_when_read() -> None:
    config = TransportConfig("amqp://rabbit:5672/?queue=jobs")

    assert isinstance(config.parsed, Dsn)
    assert config.parsed.scheme == "amqp"


def test_a_malformed_dsn_is_refused_when_the_transport_reads_it() -> None:
    """A DSN often arrives from the environment, read only when a transport is built."""
    config = TransportConfig("just-a-host")

    with pytest.raises(InvalidDsnError) as excinfo:
        _ = config.parsed

    assert excinfo.value.dsn == "just-a-host"
