"""Building ``in-memory://`` transports from a DSN."""

from __future__ import annotations

from typing import TYPE_CHECKING, final

from typing_extensions import override

from message_bus.transport.serialization import JsonSerializer
from message_bus.transport.transport_factory_interface import TransportFactoryInterface
from message_bus.transport.transport_options import as_bool, reject_unknown_options

from .in_memory_transport import InMemoryTransport

if TYPE_CHECKING:
    from collections.abc import Mapping

    from message_bus.dsn import Dsn
    from message_bus.transport.sender import SenderInterface
    from message_bus.transport.serialization import SerializerInterface
    from message_bus.transport.transport_config import TransportConfig

__all__ = ["IN_MEMORY_OPTIONS", "IN_MEMORY_SCHEME", "InMemoryTransportFactory"]

IN_MEMORY_SCHEME = "in-memory"

#: Settings an ``in-memory://`` transport accepts.
IN_MEMORY_OPTIONS = ("serialize",)


@final
class InMemoryTransportFactory(TransportFactoryInterface):
    """Builds ``in-memory://`` transports, which record instead of sending.

    Transports are built once per name and reused. A recorder is only useful
    if the code that asserts on it, the code that publishes to it, and any
    worker that drains it all hold the *same* object — rebuilding per call
    would give each of them a private queue and quietly break all three.
    """

    __slots__ = ("_made", "_serializer")

    def __init__(self, serializer: SerializerInterface | None = None) -> None:
        """Round-trip through ``serializer`` when a transport asks to."""
        self._serializer = serializer
        self._made: dict[str, InMemoryTransport] = {}

    @override
    def supports(self, dsn: Dsn) -> bool:
        """Recognise the ``in-memory`` scheme."""
        return dsn.scheme == IN_MEMORY_SCHEME

    @override
    def create(self, group: Mapping[str, TransportConfig]) -> Mapping[str, SenderInterface]:
        """Return the recorder for each name, building it the first time.

        ``?serialize=true`` makes a recorder round-trip every message through
        the serializer, which turns an unserializable field into a test
        failure rather than a production one.
        """
        return {name: self._transport(name, spec) for name, spec in group.items()}

    def _transport(self, name: str, spec: TransportConfig) -> InMemoryTransport:
        made = self._made.get(name)
        if made is None:
            made = InMemoryTransport(self._serializer_for(spec))
            self._made[name] = made
        return made

    def _serializer_for(self, spec: TransportConfig) -> SerializerInterface | None:
        """Return the serializer this transport round-trips through, if any.

        Raises:
            UnknownTransportOptionError: If the DSN carries a setting this
                transport does not accept.
        """
        reject_unknown_options(IN_MEMORY_SCHEME, spec.settings, IN_MEMORY_OPTIONS)
        if not as_bool(spec.settings, "serialize"):
            return None
        return self._serializer if self._serializer is not None else JsonSerializer()
