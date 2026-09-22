"""The routing table: which transports a message is sent to.

Routing maps a message *type* to one or more *transport names*, never to a
handler. That separation is what keeps a producer's import graph free of
consumer code, and it leaves room for a message to fan out to several
transports without changing anything here.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, final

from typing_extensions import override

from message_bus.exception import UnknownTransportError
from message_bus.message_registry import transports_of
from message_bus.stamp import TransportNamesStamp

from .senders_locator_interface import SendersLocatorInterface

if TYPE_CHECKING:
    from collections.abc import Iterator, Mapping, Sequence

    from message_bus.envelope import Envelope

    from .sender_interface import SenderInterface

__all__ = ["WILDCARD", "SendersLocator"]

WILDCARD = "*"


@final
class SendersLocator(SendersLocatorInterface):
    """Maps message types to named transports.

    Resolution, most specific first:

    1. A :class:`~message_bus.stamp.TransportNamesStamp` on the envelope,
       which overrides everything for that one dispatch.
    2. The table, walking the message's method resolution order — routing a
       base class or a shared marker class routes every subclass.
    3. The ``"*"`` catch-all entry.
    4. The ``transport`` declared on the message by
       :func:`~message_bus.decorator.as_message`.

    The table therefore always wins over what a message declares about
    itself, so an application can re-route a message it does not own.
    """

    __slots__ = ("_routes", "_senders")

    def __init__(
        self,
        routes: Mapping[type | str, str | Sequence[str]],
        senders: Mapping[str, SenderInterface],
    ) -> None:
        """Build the table and fail fast on a route naming an unknown transport."""
        self._senders = dict(senders)
        self._routes: dict[type | str, tuple[str, ...]] = {
            key: (value,) if isinstance(value, str) else tuple(value)
            for key, value in routes.items()
        }
        for names in self._routes.values():
            for name in names:
                if name not in self._senders:
                    raise UnknownTransportError(name, tuple(self._senders))

    @override
    def senders_for(self, envelope: Envelope) -> Iterator[tuple[str, SenderInterface]]:
        """Yield the senders this envelope is routed to, without duplicates."""
        seen: set[str] = set()
        for name in self._transport_names(envelope):
            if name in seen:
                continue
            seen.add(name)
            sender = self._senders.get(name)
            if sender is None:
                raise UnknownTransportError(name, tuple(self._senders))
            yield name, sender

    @override
    def routed_type_names(self) -> tuple[str, ...]:
        """Return the routed message types as readable names."""
        return tuple(
            sorted(key if isinstance(key, str) else key.__qualname__ for key in self._routes)
        )

    def _transport_names(self, envelope: Envelope) -> tuple[str, ...]:
        override_names = envelope.last(TransportNamesStamp)
        if override_names is not None:
            return override_names.transport_names
        message_type = type(envelope.message)
        matched: list[str] = []
        for base in message_type.__mro__:
            matched.extend(self._routes.get(base, ()))
        if matched:
            return tuple(matched)
        wildcard = self._routes.get(WILDCARD, ())
        if wildcard:
            return wildcard
        return transports_of(message_type)
