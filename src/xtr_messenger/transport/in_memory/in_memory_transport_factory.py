"""Building ``in-memory://`` transports from a DSN."""

from __future__ import annotations

from typing import TYPE_CHECKING, final

from typing_extensions import override

from xtr_messenger.exception import MissingSerializerError
from xtr_messenger.transport.transport_factory_interface import TransportFactoryInterface
from xtr_messenger.transport.transport_options import as_bool, reject_unknown_options

from .in_memory_transport import InMemoryTransport

if TYPE_CHECKING:
    from collections.abc import Mapping

    from xtr_messenger.dsn import Dsn
    from xtr_messenger.transport.sender import SenderInterface
    from xtr_messenger.transport.serialization import SerializerInterface
    from xtr_messenger.transport.transport_config import TransportConfig

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
        """Round-trip through ``serializer`` when a transport asks to.

        Built without one, its transports record what they are handed as it
        is, and one asking for ``?serialize=true`` is refused.
        """
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

        Raises:
            UnknownTransportOptionError: If the DSN carries a setting this
                transport does not accept.
            MissingSerializerError: If a DSN asks for ``serialize=true`` and
                this factory was built without a serializer.
        """
        return {name: self._transport(name, config) for name, config in group.items()}

    def _transport(self, name: str, config: TransportConfig) -> InMemoryTransport:
        made = self._made.get(name)
        if made is None:
            made = InMemoryTransport(self._serializer_for(config))
            self._made[name] = made
        return made

    def _serializer_for(self, config: TransportConfig) -> SerializerInterface | None:
        """Return the serializer this transport round-trips through, if any."""
        reject_unknown_options(IN_MEMORY_SCHEME, config.settings, IN_MEMORY_OPTIONS)
        if not as_bool(config.settings, "serialize"):
            return None
        if self._serializer is None:
            reason = "serialize=true round-trips through one, and this factory was built without"
            raise MissingSerializerError(IN_MEMORY_SCHEME, reason)
        return self._serializer
