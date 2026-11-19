"""``messenger:show``: the messages waiting in a transport."""

from __future__ import annotations

from collections import Counter
from typing import Annotated, final

from xtr_console import Argument, ConsoleStyle, ExitCode, Option, Range, as_command, escape

from xtr_messenger._text import class_name
from xtr_messenger.exception import MessageBusError, UnsupportedTransportOperationError
from xtr_messenger.failure.failed_message_filter import FailedMessageFilter
from xtr_messenger.failure.failed_message_repository import FailedMessageRepository
from xtr_messenger.transport.receiver.listable_receiver_interface import (
    ListableReceiverInterface,
)
from xtr_messenger.transport.transport_locator import TransportLocator

__all__ = ["ShowMessagesCommand"]

_DEFAULT_MAX = 50


@as_command("messenger:show")
@final
class ShowMessagesCommand:
    """Lists the messages waiting in a transport, or shows one of them.

    A container builds it with the transports the messenger bundle provides.
    """

    __slots__ = ("_transports",)

    def __init__(self, transports: TransportLocator) -> None:
        """Look into the transports ``transports`` hands over."""
        self._transports = transports

    async def __call__(  # noqa: PLR0913 — one parameter per option; one exit per refusal
        self,
        io: ConsoleStyle,
        transport: str | None = None,
        message_id: Annotated[str | None, Argument(name="id")] = None,
        *,
        max_messages: Annotated[int | None, Option(name="max", validator=Range(gte=1))] = None,
        stats: bool = False,
        class_filter: str | None = None,
    ) -> int:
        """Show one or more pending messages from a transport.

        Asked interactively for the transport when none is named.

        Args:
            io: Where the command writes.
            transport: The transport to inspect.
            message_id: The message to show; the pending ones are listed when
                left out.
            max_messages: How many messages to list at most — 50 when
                listing, every one with ``--stats``.
            stats: Count the pending messages by class instead.
            class_filter: Only the messages of this class — ``module.QualName``.
        """
        if message_id is not None and (stats or class_filter is not None):
            io.error('A message id cannot be combined with "--stats" or "--class-filter".')
            return ExitCode.INVALID
        names = self._transports.names()
        if transport is None:
            if not io.interactive or not names:
                io.error("Pass the name of the transport to inspect.")
                return ExitCode.INVALID
            transport = io.ask("Which transport do you want to inspect?", names[0], choices=names)
        if not self._transports.has(transport):
            known = ", ".join(escape(name) for name in names)
            io.error(f'The "{escape(transport)}" transport does not exist. Known: {known}.')
            return ExitCode.FAILURE
        try:
            receiver = self._transports.get(transport)
            if not isinstance(receiver, ListableReceiverInterface):
                raise UnsupportedTransportOperationError(transport, "list messages")
            selected = FailedMessageFilter(class_name=class_filter)
            if stats:
                await _count_by_class(io, receiver, selected, max_messages)
            elif message_id is None:
                await _list(io, receiver, transport, selected, max_messages or _DEFAULT_MAX)
            else:
                return await _show(io, receiver, message_id)
        except MessageBusError as error:
            io.error(escape(str(error)))
            return ExitCode.FAILURE
        return ExitCode.SUCCESS


async def _list(
    io: ConsoleStyle,
    receiver: ListableReceiverInterface,
    transport: str,
    selected: FailedMessageFilter,
    limit: int,
) -> None:
    if selected.class_name is not None:
        io.text(f"Displaying only '{escape(selected.class_name)}' messages.")
    rows = [
        [
            escape(FailedMessageRepository.message_id(envelope) or ""),
            escape(class_name(envelope.message)),
        ]
        async for envelope in selected.select(receiver, limit)
    ]
    if not rows:
        io.success("No pending messages were found.")
        return
    io.table(["Id", "Class"], rows)
    if len(rows) == limit:
        io.text(f"Showing first {limit} messages.")
    elif not selected.is_empty():
        io.text(f"Showing {len(rows)} message(s).")
    io.text(f"Run messenger:show {escape(transport)} {{id}} -vv to see message details.")


async def _count_by_class(
    io: ConsoleStyle,
    receiver: ListableReceiverInterface,
    selected: FailedMessageFilter,
    limit: int | None,
) -> None:
    counted = Counter([class_name(e.message) async for e in selected.select(receiver, limit)])
    if not counted:
        io.success("No pending messages were found.")
        return
    io.table(["Class", "Count"], [[escape(name), str(count)] for name, count in counted.items()])


async def _show(io: ConsoleStyle, receiver: ListableReceiverInterface, message_id: str) -> int:
    envelope = await receiver.find(message_id)
    if envelope is None:
        io.error(f'The message "{escape(message_id)}" was not found.')
        return ExitCode.FAILURE
    io.title("Message Details")
    io.table(
        ["Field", "Value"],
        [
            ["Class", escape(class_name(envelope.message))],
            ["Message Id", escape(FailedMessageRepository.message_id(envelope) or "")],
        ],
    )
    if io.is_very_verbose():
        io.section("Message")
        io.text(escape(repr(envelope.message)))
    else:
        io.text("Re-run the command with -vv to see more message details.")
    return ExitCode.SUCCESS
