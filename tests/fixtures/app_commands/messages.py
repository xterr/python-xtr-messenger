"""The application's message, and its handler."""

from __future__ import annotations

from dataclasses import dataclass

from xtr_messenger import as_message, as_message_handler


@as_message(name="tests.commands.shipped.v1")
@dataclass(frozen=True)
class Shipped:
    """A parcel left the warehouse."""

    parcel: str


@as_message_handler(Shipped)
async def record_shipment(message: Shipped) -> None:
    """Note the shipment."""
    del message
