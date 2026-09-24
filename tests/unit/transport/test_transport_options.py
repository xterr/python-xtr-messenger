from __future__ import annotations

import pytest

from message_bus import InvalidTransportOptionError, UnknownTransportOptionError
from message_bus.transport.transport_options import (
    as_bool,
    as_choice,
    as_float,
    as_int,
    as_optional_float,
    as_optional_int,
    reject_unknown_options,
)

# ─── reject_unknown_options ──────────────────────────────────────


def test_known_options_are_accepted() -> None:
    reject_unknown_options("amqp", {"queue": "jobs"}, ("queue", "prefetch_count"))


def test_an_unknown_option_is_refused_naming_what_is_accepted() -> None:
    with pytest.raises(UnknownTransportOptionError) as excinfo:
        reject_unknown_options("amqp", {"nope": "x"}, ("queue",))

    assert excinfo.value.scheme == "amqp"
    assert excinfo.value.unknown == ("nope",)
    assert excinfo.value.known == ("queue",)


def test_unknown_options_are_reported_sorted() -> None:
    with pytest.raises(UnknownTransportOptionError) as excinfo:
        reject_unknown_options("amqp", {"zeta": "1", "alpha": "2"}, ())

    assert excinfo.value.unknown == ("alpha", "zeta")


# ─── as_int ──────────────────────────────────────────────────────


def test_as_int_reads_a_whole_number() -> None:
    assert as_int({"n": "42"}, "n", 0) == 42


def test_as_int_returns_the_fallback_when_absent() -> None:
    assert as_int({}, "n", 7) == 7


def test_as_int_rejects_a_non_integer() -> None:
    with pytest.raises(InvalidTransportOptionError) as excinfo:
        _ = as_int({"n": "not a number"}, "n", 0)

    assert excinfo.value.option == "n"
    assert excinfo.value.value == "not a number"
    assert excinfo.value.expected == "a whole number"


# ─── as_float ────────────────────────────────────────────────────


def test_as_float_reads_a_number() -> None:
    assert as_float({"x": "1.5"}, "x", 0.0) == 1.5


def test_as_float_returns_the_fallback_when_absent() -> None:
    assert as_float({}, "x", 2.5) == 2.5


def test_as_float_rejects_a_non_number() -> None:
    with pytest.raises(InvalidTransportOptionError) as excinfo:
        _ = as_float({"x": "abc"}, "x", 0.0)

    assert excinfo.value.expected == "a number"


# ─── as_bool ─────────────────────────────────────────────────────


def test_as_bool_reads_true() -> None:
    assert as_bool({"flag": "true"}, "flag") is True


def test_as_bool_reads_false() -> None:
    assert as_bool({"flag": "false"}, "flag") is False


def test_as_bool_returns_the_fallback_when_absent() -> None:
    assert as_bool({}, "flag", fallback=True) is True


def test_as_bool_rejects_a_value_that_is_not_true_or_false() -> None:
    """Only 'true'/'false' — a value like 'yes' must fail rather than be read as one."""
    with pytest.raises(InvalidTransportOptionError) as excinfo:
        _ = as_bool({"flag": "yes"}, "flag")

    assert excinfo.value.expected == "'true' or 'false'"


# ─── as_choice ───────────────────────────────────────────────────


def test_as_choice_reads_an_allowed_value() -> None:
    assert as_choice({"mode": "fast"}, "mode", ("fast", "slow"), "slow") == "fast"


def test_as_choice_returns_the_fallback_when_absent() -> None:
    assert as_choice({}, "mode", ("fast", "slow"), "slow") == "slow"


def test_as_choice_rejects_a_value_outside_the_allowed_set() -> None:
    with pytest.raises(InvalidTransportOptionError) as excinfo:
        _ = as_choice({"mode": "sideways"}, "mode", ("fast", "slow"), "slow")

    assert excinfo.value.expected == "one of: fast, slow"


# ─── as_optional_int ─────────────────────────────────────────────


def test_as_optional_int_returns_the_fallback_when_absent() -> None:
    assert as_optional_int({}, "n", None) is None


def test_as_optional_int_reads_a_value_when_present() -> None:
    assert as_optional_int({"n": "5"}, "n", None) == 5


def test_as_optional_int_rejects_a_non_integer() -> None:
    with pytest.raises(InvalidTransportOptionError):
        _ = as_optional_int({"n": "x"}, "n", None)


# ─── as_optional_float ───────────────────────────────────────────


def test_as_optional_float_returns_the_fallback_when_absent() -> None:
    assert as_optional_float({}, "x", None) is None


def test_as_optional_float_reads_a_value_when_present() -> None:
    assert as_optional_float({"x": "1.5"}, "x", None) == 1.5


def test_as_optional_float_rejects_a_non_number() -> None:
    with pytest.raises(InvalidTransportOptionError):
        _ = as_optional_float({"x": "y"}, "x", None)
