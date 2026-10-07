"""Hiding credentials in a DSN before it reaches a log line or an error."""

from __future__ import annotations

import pytest

from xtr_messenger._redaction import redacted


def test_a_password_is_stripped_from_the_authority() -> None:
    assert redacted("amqp://user:s3cret@host/vh") == "amqp://host/vh"


def test_a_lone_username_is_stripped_too() -> None:
    assert redacted("amqp://user@host:5672/vh") == "amqp://host:5672/vh"


def test_a_dsn_without_credentials_is_unchanged() -> None:
    assert redacted("amqp://host:5672/vh?queue=jobs") == "amqp://host:5672/vh?queue=jobs"


def test_a_numeric_password_posing_as_a_port_is_dropped() -> None:
    """``amqp://user:12345`` is what ``Dsn.connection`` keeps of a DSN whose
    password lost its tail to a ``?`` or ``#`` — the digits are a password,
    not a port, and a bare dotless authority with nothing after it reads as
    credentials."""
    assert "12345" not in redacted("amqp://user:12345")


def test_a_bare_numeric_password_pair_is_masked_whole() -> None:
    """``u:12345`` has no host to keep at all; both halves are credentials."""
    assert redacted("u:12345") == "<redacted dsn>"


def test_a_local_broker_with_a_port_is_kept() -> None:
    """``localhost`` reads as a server, never a username, so the canonical
    local DSN stays readable."""
    assert redacted("amqp://localhost:5672") == "amqp://localhost:5672"


def test_a_dotted_host_with_a_port_and_no_path_is_kept() -> None:
    assert redacted("amqp://mq.example.com:5672") == "amqp://mq.example.com:5672"


def test_a_dotless_host_followed_by_a_path_is_kept() -> None:
    """A real ``host:port`` DSN goes on to a path or options; only a bare
    authority is read as a credential pair."""
    assert redacted("amqp://rabbit:5672/vh") == "amqp://rabbit:5672/vh"


def test_a_hostless_dsn_is_unchanged() -> None:
    assert redacted("sync://") == "sync://"


def test_a_hostless_dsn_with_options_is_unchanged() -> None:
    assert redacted("in-memory://?serialize=true") == "in-memory://?serialize=true"


def test_credentials_without_a_scheme_leave_only_the_host() -> None:
    """The shape an InvalidDsnError reports: credentials and no ``://`` at all."""
    assert redacted("user:s3cret@host/vh") == "host/vh"


def test_credentials_behind_a_bare_scheme_colon_leave_only_the_host() -> None:
    assert redacted("amqp:user:s3cret@host/vh") == "host/vh"


def test_credentials_behind_an_empty_scheme_lose_the_separator() -> None:
    """``://`` with no scheme in front of it is not a ``scheme://`` prefix:
    nothing before the host is kept, so the empty ``://`` goes with it."""
    assert redacted("://user:s3cret@host/vh") == "host/vh"


def test_credentials_with_no_path_leave_only_the_host() -> None:
    assert redacted("user:s3cret@host") == "host"


def test_an_unparsable_dsn_is_masked_whole() -> None:
    """``urlsplit`` raises on an unterminated IPv6 host; returning the DSN as
    written would publish the password the parse failed on."""
    assert redacted("amqp://user:s3cret@[::1/vh") == "<redacted dsn>"


def test_an_empty_userinfo_prefix_leaving_a_bare_pair_is_masked_whole() -> None:
    """A leading ``@`` makes ``urlsplit`` see no authority, so the textual pass
    drops through it and is left with ``user:s3cret`` — a credential pair with
    no host to keep. Returning it would publish the password."""
    assert redacted("@user:s3cret") == "<redacted dsn>"


#: The DSN shapes a well-formed authority never catches, each paired with the
#: fragments of its password that redaction must leave behind in nothing it
#: renders. ``urlsplit`` reads a netloc with no ``@``, so the credentials slip
#: past into the path, the query or the fragment; ``@`` only survives after an
#: authority whose first segment had none; or there is no ``@`` at all and the
#: colon alone separates a username from its password.
_LEAKY_DSNS = (
    ("amqp://user:s3/cret@host/vh", ("s3", "cret")),
    ("amqp://user:s3?cret@host/vh", ("s3", "cret")),
    ("amqp://user:s3#cret@host/vh", ("s3", "cret")),
    ("user:s3/cret@host/vh", ("s3", "cret")),
    ("user:s3cret@host/vh?next=amqp://x", ("s3", "cret")),
    ("user:s3cret", ("s3", "cret")),
    ("rabbit:guest", ("guest",)),
)


@pytest.mark.parametrize(("dsn", "secrets"), _LEAKY_DSNS)
def test_credentials_the_authority_path_misses_are_redacted(
    dsn: str,
    secrets: tuple[str, ...],
) -> None:
    """A netloc with no ``@`` left the password in the path, query or fragment,
    and a trailing ``scheme://`` in the query made the whole prefix look kept.
    Whenever an ``@`` survives the authority pass, redaction drops through the
    last one, over-redacting rather than leaking. A pair with no ``@`` at all
    reads as a scheme and a path, so it is masked whole instead."""
    rendered = redacted(dsn)

    for secret in secrets:
        assert secret not in rendered
