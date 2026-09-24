"""How the connection to the broker is opened.

These are the AMQP URI parameters the driver reads for itself — heartbeat,
frame limits, TLS material. They are written back onto the connection URL
rather than passed as keyword arguments, because that is where ``aiormq``
looks for them, and it keeps one code path for a setting whether it arrived
in the DSN or in ``options``.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Final
from urllib.parse import urlencode, urlsplit, urlunsplit

from message_bus.transport.transport_options import (
    as_bool,
    as_optional_float,
    as_optional_int,
)

if TYPE_CHECKING:
    from collections.abc import Mapping

__all__ = ["CONNECTION_OPTIONS", "ConnectionOptions"]

#: Settings :meth:`ConnectionOptions.from_settings` reads.
CONNECTION_OPTIONS: Final = (
    "heartbeat",
    "connect_timeout",
    "connection_name",
    "frame_max",
    "channel_max",
    "cacert",
    "cert",
    "key",
    "verify",
)


@dataclass(frozen=True, slots=True)
class ConnectionOptions:
    """How the driver opens and keeps the connection.

    Credentials and the host are not here — a URL already expresses those,
    and they stay in the DSN.

    Attributes:
        heartbeat: Seconds between heartbeats. Detects a dead peer that never
            sent a close frame.
        connect_timeout: Seconds to wait for the connection to open.
        connection_name: What this connection calls itself in RabbitMQ's
            management UI, which is what makes one process distinguishable
            from another there.
        frame_max: Largest frame the connection will accept, in bytes.
        channel_max: Most channels the connection will open.
        cacert: Path to the CA bundle that signs the broker's certificate.
        cert: Path to this client's certificate, for mutual TLS.
        key: Path to this client's private key.
        verify: Whether the broker's certificate is checked. Turning this off
            keeps the encryption and discards the identity check with it.
    """

    heartbeat: float | None = None
    connect_timeout: float | None = None
    connection_name: str | None = None
    frame_max: int | None = None
    channel_max: int | None = None
    cacert: str | None = None
    cert: str | None = None
    key: str | None = None
    verify: bool = True

    @classmethod
    def from_settings(
        cls,
        settings: Mapping[str, str],
        defaults: ConnectionOptions | None = None,
    ) -> ConnectionOptions:
        """Read the connection settings, falling back to ``defaults``.

        Raises:
            InvalidTransportOptionError: If a value is present but not usable.
        """
        base = defaults if defaults is not None else cls()
        return cls(
            heartbeat=as_optional_float(settings, "heartbeat", base.heartbeat),
            connect_timeout=as_optional_float(settings, "connect_timeout", base.connect_timeout),
            connection_name=settings.get("connection_name", base.connection_name),
            frame_max=as_optional_int(settings, "frame_max", base.frame_max),
            channel_max=as_optional_int(settings, "channel_max", base.channel_max),
            cacert=settings.get("cacert", base.cacert),
            cert=settings.get("cert", base.cert),
            key=settings.get("key", base.key),
            verify=as_bool(settings, "verify", base.verify),
        )

    def applied_to(self, url: str) -> str:
        """Return ``url`` carrying these settings as AMQP URI parameters.

        Anything already in the URL's own query string is kept, so a DSN that
        was written with driver parameters still works.
        """
        parameters = {
            "heartbeat": _seconds(self.heartbeat),
            "connection_timeout": _seconds(self.connect_timeout),
            "name": self.connection_name,
            "frame_max": _whole(self.frame_max),
            "channel_max": _whole(self.channel_max),
            "cafile": self.cacert,
            "certfile": self.cert,
            "keyfile": self.key,
            "no_verify_ssl": None if self.verify else "1",
        }
        given = {key: value for key, value in parameters.items() if value is not None}
        if not given:
            return url
        split = urlsplit(url)
        query = f"{split.query}&{urlencode(given)}" if split.query else urlencode(given)
        return urlunsplit(split._replace(query=query))


def _seconds(value: float | None) -> str | None:
    return None if value is None else str(int(value))


def _whole(value: int | None) -> str | None:
    return None if value is None else str(value)
