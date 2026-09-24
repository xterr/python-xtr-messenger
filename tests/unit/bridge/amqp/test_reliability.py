"""How hard to retry, and where a message goes when trying stops."""

from __future__ import annotations

import pytest

from xtr_messenger import InvalidTransportOptionError
from xtr_messenger.bridge.amqp.reliability import DEFAULT_DEAD_LETTER_QUEUE, Reliability


def test_the_defaults() -> None:
    reliability = Reliability()

    assert reliability.max_attempts == 3
    assert reliability.base_delay_seconds == 1.0
    assert reliability.dead_letter_queue == DEFAULT_DEAD_LETTER_QUEUE


def test_the_default_dead_letter_queue() -> None:
    assert DEFAULT_DEAD_LETTER_QUEUE == "taskiq.dlq"


def test_from_settings_reads_every_field() -> None:
    reliability = Reliability.from_settings(
        {"max_attempts": "10", "base_delay_seconds": "2.5", "dead_letter_queue": "my.dlq"},
    )

    assert reliability.max_attempts == 10
    assert reliability.base_delay_seconds == 2.5
    assert reliability.dead_letter_queue == "my.dlq"


def test_settings_override_the_given_defaults() -> None:
    """Defaults come from code; a setting overrides one without touching the rest."""
    built = Reliability.from_settings(
        {"max_attempts": "7"},
        Reliability(max_attempts=1, base_delay_seconds=9.0),
    )

    assert built.max_attempts == 7
    assert built.base_delay_seconds == 9.0


def test_an_empty_dead_letter_queue_disables_dead_lettering() -> None:
    built = Reliability.from_settings({"dead_letter_queue": ""})

    assert built.dead_letter_queue is None


def test_a_non_numeric_max_attempts_is_refused() -> None:
    with pytest.raises(InvalidTransportOptionError, match="max_attempts"):
        _ = Reliability.from_settings({"max_attempts": "lots"})


def test_a_non_numeric_base_delay_is_refused() -> None:
    with pytest.raises(InvalidTransportOptionError, match="base_delay_seconds"):
        _ = Reliability.from_settings({"base_delay_seconds": "slow"})
