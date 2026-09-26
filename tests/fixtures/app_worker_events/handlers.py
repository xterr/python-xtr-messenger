"""The application's handler."""

from __future__ import annotations

from xtr_messenger import as_message_handler

from .messages import Ping


@as_message_handler(Ping)
async def handle_ping(message: Ping) -> None:
    """Accept every ping."""
    del message
