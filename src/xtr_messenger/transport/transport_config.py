"""How to reach one named transport."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType

from xtr_messenger.dsn import Dsn

__all__ = ["TransportConfig"]


def _no_options() -> Mapping[str, str]:
    return MappingProxyType({})


@dataclass(frozen=True, slots=True)
class TransportConfig:
    """Configuration for a single named transport.

    Adapter settings can be written either in the DSN's query string or in
    ``options``. A DSN travels in one environment variable, which suits
    deployment; ``options`` survives review better once there are more than a
    couple, and can be built from code. Both reach the adapter through
    :attr:`settings`, so a setting can move between them without changing
    meaning.

    Attributes:
        dsn: Selects the adapter and carries its connection details —
            ``sync://``, ``in-memory://`` or ``amqp://…``.
        queue: The broker queue this transport reads and writes. Transports
            sharing a DSN but naming different queues are what lets a worker
            consume one workload and ignore the rest. May also be given in
            the DSN as ``?queue=…``; this field wins when both are present.
        options: Adapter settings, overriding any of the same name in the
            DSN. Values are strings so that both sources read alike and a
            setting can move between them without changing meaning.
    """

    dsn: str
    queue: str | None = None
    options: Mapping[str, str] = field(default_factory=_no_options)

    @property
    def parsed(self) -> Dsn:
        """The DSN, read when a transport is built from it.

        Not read when the config is written: a DSN usually comes from an
        environment variable, which a container reads only when the transport
        is needed.

        Raises:
            InvalidDsnError: If the DSN carries no scheme.
        """
        return Dsn.parse(self.dsn)

    @property
    def settings(self) -> Mapping[str, str]:
        """Every adapter setting, from the DSN and from ``options``, read-only.

        ``options`` wins on conflict, so a DSN held in an environment variable
        can be overridden in code without editing it.

        Raises:
            InvalidDsnError: If the DSN carries no scheme.
        """
        merged = dict(self.parsed.options)
        merged.update(self.options)
        if self.queue is not None:
            merged["queue"] = self.queue
        return MappingProxyType(merged)

    @property
    def queue_name(self) -> str | None:
        """Return the queue this transport uses, from the field or the DSN."""
        return self.settings.get("queue")
