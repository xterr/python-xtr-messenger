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


def test_repr_hides_the_dsn_credentials() -> None:
    rendered = repr(Dsn.parse("amqp://user:s3cret@host/vh"))

    assert "s3cret" not in rendered


def test_repr_neither_raises_nor_leaks_on_an_unreadable_dsn() -> None:
    """A traceback renders whatever a frame holds. A repr that raised while
    hiding the password would replace the diagnosis with its own crash."""
    unreadable = Dsn(
        raw="amqp://user:s3cret@[::1/vh",
        scheme="amqp",
        connection="amqp://user:s3cret@[::1/vh",
        options=MappingProxyType({}),
    )

    rendered = repr(unreadable)

    assert "s3cret" not in rendered


#: Credentials with an unescaped ``/``, ``?`` or ``#``: ``urlsplit`` ends the
#: authority early, so part of the password slips into the path, the options
#: or the fragment — and from there into every place options are rendered.
_SPLIT_PASSWORD_DSNS = (
    "amqp://user:s3/cret@host/vh",
    "amqp://user:s3?cret@host/vh",
    "amqp://user:s3#cret@host/vh",
)


@pytest.mark.parametrize("dsn", _SPLIT_PASSWORD_DSNS)
def test_a_dsn_with_credentials_beyond_its_authority_is_refused(dsn: str) -> None:
    """Userinfo must be percent-encoded: read as written, part of the password
    would become options — rendered by ``repr`` and option errors verbatim."""
    with pytest.raises(InvalidDsnError) as excinfo:
        _ = Dsn.parse(dsn)

    assert "percent-encode" in str(excinfo.value)
    assert "s3" not in str(excinfo.value)
    assert "cret" not in str(excinfo.value)


def test_an_escaped_password_with_reserved_characters_is_accepted() -> None:
    dsn = Dsn.parse("amqp://user:s3%3Fcret@host/vh?queue=jobs")

    assert dsn.options["queue"] == "jobs"


def test_a_dsn_urlsplit_refuses_is_an_invalid_dsn() -> None:
    """An unterminated IPv6 host raises ``ValueError`` deep in URL parsing;
    callers of ``parse`` are promised ``InvalidDsnError``, redacted."""
    with pytest.raises(InvalidDsnError) as excinfo:
        _ = Dsn.parse("amqp://user:s3cret@[::1/vh")

    assert "s3cret" not in str(excinfo.value)
