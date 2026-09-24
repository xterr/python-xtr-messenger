from __future__ import annotations

from types import MappingProxyType

import pytest

from xtr_messenger import Dsn, InvalidDsnError


def test_a_scheme_is_read_from_a_hostless_dsn() -> None:
    assert Dsn.parse("sync://").scheme == "sync"


def test_a_hyphenated_scheme_survives() -> None:
    assert Dsn.parse("in-memory://").scheme == "in-memory"


def test_a_scheme_is_lowercased() -> None:
    assert Dsn.parse("AMQP://host").scheme == "amqp"


def test_query_options_are_decoded() -> None:
    dsn = Dsn.parse("amqp://rabbit:5672/%2f?queue=jobs_high&priority=5")

    assert dsn.options["queue"] == "jobs_high"
    assert dsn.options["priority"] == "5"


def test_an_absent_option_is_none() -> None:
    assert Dsn.parse("sync://").options.get("queue") is None


def test_the_raw_dsn_is_kept_for_the_adapter() -> None:
    raw = "amqp://guest:guest@rabbit:5672/%2f"

    assert Dsn.parse(raw).raw == raw


def test_a_dsn_without_a_separator_is_refused() -> None:
    with pytest.raises(InvalidDsnError) as excinfo:
        _ = Dsn.parse("just-a-host")

    assert excinfo.value.dsn == "just-a-host"


def test_a_dsn_with_a_separator_but_no_scheme_is_refused() -> None:
    """``://host`` has the separator yet no scheme to dispatch on."""
    with pytest.raises(InvalidDsnError) as excinfo:
        _ = Dsn.parse("://host")

    assert excinfo.value.dsn == "://host"


def test_options_are_a_read_only_view() -> None:
    assert isinstance(Dsn.parse("sync://?a=1").options, MappingProxyType)


def test_the_connection_drops_the_options_but_stays_a_valid_dsn() -> None:
    dsn = Dsn.parse("amqp://rabbit:5672/%2f?queue=jobs_low")

    assert dsn.connection == "amqp://rabbit:5672/%2f"
    assert Dsn.parse(dsn.connection).scheme == "amqp"


def test_two_transports_differing_only_in_options_share_a_connection() -> None:
    plain = Dsn.parse("amqp://rabbit:5672/%2f")
    with_queue = Dsn.parse("amqp://rabbit:5672/%2f?queue=jobs_low")

    assert plain.connection == with_queue.connection


def test_a_hostless_connection_stays_parseable() -> None:
    assert Dsn.parse(Dsn.parse("in-memory://").connection).scheme == "in-memory"
