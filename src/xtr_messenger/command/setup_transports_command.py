"""``messenger:setup-transports``: create what each transport needs before it is used."""

from __future__ import annotations

from typing import final

from xtr_console import ConsoleStyle, ExitCode, as_command, escape

from xtr_messenger.exception import MessageBusError
from xtr_messenger.transport.setupable_transport_interface import SetupableTransportInterface
from xtr_messenger.transport.transport_locator import TransportLocator

__all__ = ["SetupTransportsCommand"]


@as_command("messenger:setup-transports")
@final
class SetupTransportsCommand:
    """Prepares the infrastructure each transport needs — its table, its queues.

    A container builds it with the transports the messenger bundle provides.
    """

    __slots__ = ("_transports",)

    def __init__(self, transports: TransportLocator) -> None:
        """Set up the transports ``transports`` hands over."""
        self._transports = transports

    async def __call__(self, io: ConsoleStyle, transport: str | None = None) -> int:
        """Prepare the required infrastructure for the transports.

        A transport that has nothing to prepare is noted and skipped.

        Args:
            io: Where the command writes.
            transport: The transport to set up; every one when left out.
        """
        names = self._transports.names()
        if transport is not None:
            if not self._transports.has(transport):
                io.error(f'The "{escape(transport)}" transport does not exist.')
                return ExitCode.FAILURE
            names = (transport,)
        for name in names:
            shown = escape(name)
            try:
                found = self._transports.get(name)
                if not isinstance(found, SetupableTransportInterface):
                    io.note(f'The "{shown}" transport does not support setup.')
                    continue
                await found.setup()
            except MessageBusError as error:
                io.error(f'Setting up the "{shown}" transport failed: {escape(str(error))}')
                return ExitCode.FAILURE
            io.success(f'The "{shown}" transport was set up successfully.')
        return ExitCode.SUCCESS
