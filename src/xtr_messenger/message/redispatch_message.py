"""A message asking for another envelope to be dispatched again."""

from __future__ import annotations

from dataclasses import dataclass
from typing import final

from typing_extensions import override

from xtr_messenger.envelope import Envelope

__all__ = ["RedispatchMessage"]


@final
@dataclass(frozen=True, slots=True, init=False)
class RedispatchMessage:
    """Dispatch ``envelope`` again, through the routing a publishing bus applies.

    Handling this message dispatches the envelope it carries, so whatever
    produced it — a scheduler generating messages, say — decides only *that*
    a message goes out, and routing decides where. ``transport_names``
    overrides routing for that one dispatch; left empty, the routing table
    decides, as it would for any dispatch.

    It holds an envelope, which no codec carries, so it lives only in the
    process that dispatches it: handle it where it is created rather than
    routing it to a transport.
    """

    envelope: Envelope | object
    transport_names: tuple[str, ...]

    def __init__(
        self,
        envelope: Envelope | object,
        transport_names: str | tuple[str, ...] | list[str] = (),
    ) -> None:
        """Carry ``envelope`` — or a bare message — and where to send it, if not by routing.

        Empty transport names are dropped: ``""`` means *no override*, not
        *a transport named nothing*.
        """
        names = (transport_names,) if isinstance(transport_names, str) else transport_names
        object.__setattr__(self, "envelope", envelope)
        object.__setattr__(self, "transport_names", tuple(name for name in names if name))

    @property
    def message(self) -> object:
        """Return the message to be dispatched again, unwrapped from its envelope."""
        return self.envelope.message if isinstance(self.envelope, Envelope) else self.envelope

    @override
    def __str__(self) -> str:
        """Describe the message, and the transports it goes to when they are named."""
        message = self.message
        own_str = type(message).__str__ is not object.__str__
        description = str(message) if own_str else type(message).__qualname__
        if not self.transport_names:
            return description
        return f"{description} via {', '.join(self.transport_names)}"
