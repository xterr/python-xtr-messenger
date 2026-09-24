"""Parsing the DSN that selects and configures a transport.

A DSN carries the scheme a factory dispatches on, plus per-transport options
in its query string::

    sync://
    in-memory://?serialize=true
    amqp://guest:guest@rabbit:5672/%2f?queue=jobs_high

Built on :func:`urllib.parse.urlsplit`, which handles hyphenated schemes,
credentials, an encoded vhost path, and a DSN with no host at all.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType
from urllib.parse import parse_qsl, urlsplit

from .exception import InvalidDsnError

__all__ = ["Dsn", "InvalidDsnError"]

_SEPARATOR = "://"


@dataclass(frozen=True, slots=True)
class Dsn:
    """A parsed transport DSN.

    Attributes:
        raw: The DSN as written, which adapters pass to their own client.
        scheme: What a factory dispatches on, lowercased.
        connection: The DSN without its options, still a usable DSN. Two
            transports that differ
            only in their query string address the same server, so this is
            what decides whether they can share a connection.
        options: Decoded query-string parameters.
    """

    raw: str
    scheme: str
    connection: str
    options: Mapping[str, str]

    @classmethod
    def parse(cls, raw: str) -> Dsn:
        """Read ``raw`` into its scheme, connection and options.

        Raises:
            InvalidDsnError: If ``raw`` carries no scheme.
        """
        if _SEPARATOR not in raw:
            raise InvalidDsnError(raw)
        split = urlsplit(raw)
        if not split.scheme:
            raise InvalidDsnError(raw)
        return cls(
            raw=raw,
            scheme=split.scheme.lower(),
            connection=f"{split.scheme.lower()}{_SEPARATOR}{split.netloc}{split.path}",
            options=MappingProxyType(dict(parse_qsl(split.query))),
        )
