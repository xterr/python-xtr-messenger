"""``messenger:stats``: how many messages wait in each transport."""

from __future__ import annotations

import json
from typing import Annotated, Literal, final

from xtr_console import ConsoleStyle, ExitCode, Option, as_command, escape

from xtr_messenger.exception import MessageBusError
from xtr_messenger.transport.receiver.message_count_aware_interface import (
    MessageCountAwareInterface,
)
from xtr_messenger.transport.transport_locator import TransportLocator

__all__ = ["StatsCommand"]


@as_command("messenger:stats")
@final
class StatsCommand:
    """Counts the messages waiting in each transport.

    A container builds it with the transports the messenger bundle provides.
    """

    __slots__ = ("_transports",)

    def __init__(self, transports: TransportLocator) -> None:
        """Count the messages of the transports ``transports`` hands over."""
        self._transports = transports

    async def __call__(
        self,
        io: ConsoleStyle,
        *transport_names: str,
        output_format: Annotated[Literal["txt", "json"], Option(name="format")] = "txt",
    ) -> int:
        """Show the message count for one or more transports.

        A transport that cannot count its messages is listed apart.

        Args:
            io: Where the command writes.
            transport_names: The transports to count; every one when none is named.
            output_format: ``txt`` for a table, ``json`` for a document.
        """
        counts: dict[str, int] = {}
        uncountable: list[str] = []
        for name in transport_names or self._transports.names():
            if not self._transports.has(name):
                if output_format == "txt":
                    io.warning(f'The "{escape(name)}" transport does not exist.')
                continue
            try:
                found = self._transports.get(name)
                if not isinstance(found, MessageCountAwareInterface):
                    uncountable.append(name)
                    continue
                counts[name] = await found.count()
            except MessageBusError as error:
                io.error(f'Counting the "{escape(name)}" transport failed: {escape(str(error))}')
                return ExitCode.FAILURE
        if output_format == "json":
            document: dict[str, object] = {
                "transports": {name: {"count": count} for name, count in counts.items()}
            }
            if uncountable:
                document["uncountable_transports"] = uncountable
            io.console.out(json.dumps(document, indent=4), highlight=False)
            return ExitCode.SUCCESS
        io.table(["Transport", "Count"], [[escape(n), str(c)] for n, c in counts.items()])
        if uncountable:
            listed = '", "'.join(escape(name) for name in uncountable)
            io.note(f'Unable to get message count for the following transports: "{listed}".')
        return ExitCode.SUCCESS
