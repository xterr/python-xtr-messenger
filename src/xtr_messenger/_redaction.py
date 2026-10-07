"""Hiding credentials in a DSN before it reaches a log line or an error.

A DSN carries its credentials in the authority, ``user:password@host``. That
is convenient to configure and dangerous to print: an error message, a
``repr`` in a traceback, or a debug dump all render the DSN verbatim unless
something strips the secret first. One helper does that, in one place, so
every message and ``repr`` that shows a DSN shows the same redacted form.

The helper is deliberately **textual and total**. The DSNs that most need
redacting are the malformed ones — a missing ``://``, a stray scheme, an
unterminated IPv6 host — because those are what
:class:`~xtr_messenger.exception.InvalidDsnError` reports, and a parser-shaped
rule has nothing to work with on exactly those. So nothing here depends on the
DSN parsing into an authority, and nothing here raises: a string it cannot
make sense of is masked whole rather than returned as written.

One limitation is deliberate: a credentialed DSN nested inside a query value
is not redacted, only the outer one is, so ``amqp://host/vh?next=amqp://u:p``
keeps the inner pair.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Final
from urllib.parse import urlsplit, urlunsplit

if TYPE_CHECKING:
    from urllib.parse import SplitResult

__all__ = ["redacted"]

#: What a DSN too malformed to read at all renders as. A placeholder, not the
#: original: the strings ``urlsplit`` refuses are exactly the ones nobody has
#: inspected, so there is no telling which part of them is the password.
_MASKED: Final = "<redacted dsn>"

#: A scheme and its ``://``, anchored at the start: ``amqp://``, ``in-memory://``.
#: Only such a prefix is kept across redaction — ``[A-Za-z][A-Za-z0-9+.-]*`` is
#: the character set a DSN scheme is allowed, so anything else before an ``@``
#: (a bare ``scheme:``, a ``user:password``) is dropped with the credentials
#: rather than mistaken for a scheme worth preserving.
_SCHEME_PREFIX: Final = re.compile(r"^[A-Za-z][A-Za-z0-9+.-]*://")


def redacted(dsn: str) -> str:
    """Return ``dsn`` with any ``user:password@`` credentials removed.

    For a DSN with an authority — every well-formed one — the scheme, host,
    port, path and query are kept byte-exact and only the userinfo in front of
    the host is dropped, since that is where a password travels. For one
    without, everything in front of the host goes, a bare ``scheme:`` included:
    there is no telling that prefix apart from a username. When there is no
    host left to keep at all — a lone ``user:s3cret`` — the whole string is
    masked. A DSN carrying no credentials comes back unchanged either way, so a
    hostless ``sync://``, a bare ``sync`` or a plain ``amqp://host`` reads the
    same before and after.

    Never raises, and never returns the original string when it could not be
    read: both are the point. This is called from ``__repr__`` and from error
    constructors, where a second exception would replace the diagnosis with a
    crash, and a passthrough would publish the secret.
    """
    try:
        return _without_credentials(dsn)
    except ValueError:
        return _MASKED


def _without_credentials(dsn: str) -> str:
    """Drop the credentials from ``dsn``, by authority when there is one.

    A well-formed DSN has its userinfo in the authority ``urlsplit`` reports,
    and replacing it there keeps every other component byte-exact.

    Everything else — no authority, or an authority ``urlsplit`` read without
    an ``@`` while an ``@`` survives in the path, query or fragment — falls to
    :func:`_textually_without_credentials`.

    Raises:
        ValueError: If ``dsn`` is malformed enough that ``urlsplit`` refuses
            it — an unterminated IPv6 host, say.
    """
    split = urlsplit(dsn)
    if split.netloc and "@" in split.netloc:
        host = split.netloc.rsplit("@", 1)[1]
        return urlunsplit(split._replace(netloc=host))
    if "@" in dsn:
        # The authority pass found no ``user:password@`` to drop, yet an ``@``
        # survives — a password with a ``/``, ``?`` or ``#`` makes ``urlsplit``
        # read a netloc without its ``@`` and leave the rest in the path,
        # query or fragment, and a DSN with no ``://`` keeps everything in one
        # string. Drop through the last ``@`` textually.
        return _textually_without_credentials(dsn)
    if split.netloc and _is_userinfo_shaped(split.netloc):
        # No ``@`` left, but ``urlsplit`` read a netloc of ``user:password``
        # form — the shape ``amqp://user:s3`` that a ``Dsn.connection`` keeps
        # once the ``?cret@host`` query it could not hold is stripped. There
        # is no host to keep, so the whole netloc goes: over-redacting a
        # would-be host is the safe choice when it may be a password instead.
        return urlunsplit(split._replace(netloc=""))
    if split.netloc and _is_numeric_password_pair(split):
        # The same stripped shape with a password that happens to be numeric:
        # ``amqp://user:12345`` reads as ``host:port`` to the check above.
        # Only a bare authority is treated so — a real ``host:port`` DSN goes
        # on to a path or options — and a dotted or bracketed host, or
        # ``localhost``, is kept: those read as servers, not usernames.
        return urlunsplit(split._replace(netloc=""))
    if _is_bare_userinfo(split):
        # No ``@`` and no authority either, yet ``urlsplit`` still split the
        # string on a colon: ``user:s3cret`` reads as the scheme ``user`` and
        # the path ``s3cret``. That is the shape an ``InvalidDsnError`` reports
        # for a DSN someone wrote without its ``://``, and there is no host in
        # it to keep — both halves are credentials — so it is masked whole.
        return _MASKED
    return dsn


def _textually_without_credentials(dsn: str) -> str:
    """Drop everything up to the last ``@`` left in ``dsn``.

    The caller has already found an ``@`` the authority pass did not account
    for: a password with a ``/``, ``?`` or ``#`` makes ``urlsplit`` read a
    netloc without its ``@`` and leave the rest in the path, query or
    fragment; a DSN with no ``://`` at all keeps everything in one string.

    So the rule is: a ``scheme://`` anchored at the very start is kept; then
    everything through the **last** ``@`` is dropped, credentials and all.
    Over-redacting — dropping a path segment that merely contained an ``@`` —
    is acceptable; leaking a password is not.

    Only ``scheme://`` counts as a prefix worth keeping. A lone ``scheme:`` is
    indistinguishable from a username followed by its password, so it too is
    dropped with the credentials rather than guessed at.
    """
    prefix = _SCHEME_PREFIX.match(dsn)
    start = prefix.end() if prefix is not None else 0
    rest = dsn[start:]
    at = rest.rfind("@")
    tail = rest[at + 1 :]
    if prefix is None and _is_bare_userinfo_tail(tail):
        # Nothing before the ``@`` was a ``scheme://`` to keep, and what is
        # left behind it is itself ``user:password`` shaped with no host —
        # ``@user:s3cret`` drops to ``user:s3cret``. Both halves are
        # credentials, so mask it whole rather than render the password.
        return _MASKED
    return dsn[:start] + tail


def _is_userinfo_shaped(netloc: str) -> bool:
    """Tell whether ``netloc`` looks like ``user:password`` rather than ``host:port``.

    A ``host:port`` has digits after its last colon; anything else after a
    colon is read as a password, so the netloc is credentials with no host.
    """
    if ":" not in netloc:
        return False
    _, _, after = netloc.rpartition(":")
    return not after.isdigit()


def _is_numeric_password_pair(split: SplitResult) -> bool:
    """Tell whether a bare netloc is ``user:12345`` — a numeric password posing as a port.

    Deliberately not folded into :func:`_is_userinfo_shaped`: after an ``@``
    a ``host:5672`` tail really is a host, so this stricter reading applies
    only where no ``@`` survived at all. It asks that nothing follow the
    authority — the shape ``Dsn.connection`` keeps of a DSN whose password
    lost its tail to a ``?`` or ``#`` — and that the would-be host read as a
    username: no dot, no bracket, and not ``localhost``.
    """
    if split.path or split.query or split.fragment:
        return False
    return _is_user_like_pair(split.netloc)


def _is_user_like_pair(pair: str) -> bool:
    """Tell whether ``pair`` is ``token:digits`` with a token that reads as a username."""
    before, colon, after = pair.rpartition(":")
    if not colon or not after.isdigit() or not before:
        return False
    return "." not in before and "[" not in before and before.lower() != "localhost"


def _is_bare_userinfo_tail(tail: str) -> bool:
    """Tell whether ``tail`` is a lone ``user:password`` with no host around it.

    What survives the last ``@`` is credentials with no host only when it has
    the ``user:password`` shape and nothing a host would bring: a ``/``, ``?``
    or ``#`` would start a path, query or fragment — a host's territory — so a
    tail carrying one is a host to keep, not a pair to mask.
    """
    if "/" in tail or "?" in tail or "#" in tail:
        return False
    return _is_userinfo_shaped(tail)


def _is_bare_userinfo(split: SplitResult) -> bool:
    """Tell whether ``split`` is a lone ``user:password`` and nothing else.

    ``urlsplit`` reads ``user:s3cret`` as a scheme and a path, so the authority
    pass sees no netloc and the textual pass sees no ``@`` — the string would
    otherwise come back as written, password included.

    The shape only counts as credentials when nothing else in the string
    contradicts it: a non-empty path, so ``sync://`` and ``in-memory://?x=1``
    stay readable; no ``/`` in that path, so ``mailto:a/b`` is a real scheme
    and path; and nothing that reads as a port, so ``host:5672`` is a host. A
    string with no colon at all — ``sync``, ``in-memory`` — never gets here,
    since ``urlsplit`` leaves it no scheme.
    """
    if split.netloc or not split.scheme or not split.path or "/" in split.path:
        return False
    pair = f"{split.scheme}:{split.path}"
    return _is_userinfo_shaped(pair) or _is_user_like_pair(pair)
