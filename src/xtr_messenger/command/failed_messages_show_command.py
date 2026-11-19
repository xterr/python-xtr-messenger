"""``messenger:failed:show``: the messages kept in a failure transport."""

from __future__ import annotations

from collections import Counter
from typing import Annotated, final

from xtr_console import Argument, ConsoleStyle, ExitCode, Option, Range, as_command, escape

from xtr_messenger._text import class_name
from xtr_messenger.exception import MessageBusError
from xtr_messenger.failure.failed_message_filter import FailedMessageFilter
from xtr_messenger.failure.failed_message_repository import FailedMessageRepository
from xtr_messenger.stamp import ErrorDetailsStamp, SentToFailureTransportStamp

from .abstract_failed_messages_command import (
    AbstractFailedMessagesCommand,
    InvalidInputError,
    failed_message_filter,
    moment_text,
)

__all__ = ["FailedMessagesShowCommand"]

_DEFAULT_MAX = 50


@as_command("messenger:failed:show")
@final
class FailedMessagesShowCommand(AbstractFailedMessagesCommand):
    """Lists the messages kept in a failure transport, or shows one of them."""

    async def __call__(  # noqa: PLR0913 — one parameter per argument and option of the command
        self,
        io: ConsoleStyle,
        message_id: Annotated[str | None, Argument(name="id")] = None,
        *,
        max_messages: Annotated[int | None, Option(name="max", validator=Range(gte=1))] = None,
        transport: str | None = None,
        stats: bool = False,
        class_filter: str | None = None,
        failed_after: str | None = None,
        failed_before: str | None = None,
    ) -> int:
        """Show one or more messages from the failure transport.

        The filters accept an ISO-8601 date or a modifier — ``--failed-after='-1 hour'``;
        both bounds are inclusive, and a message with no known failure time is
        never selected by them. They narrow ``--stats`` down too, and cannot be
        combined with an id.

        Args:
            io: Where the command writes.
            message_id: The message to show; the failed ones are listed when
                left out.
            max_messages: How many messages to list at most — 50 when
                listing, every one with ``--stats``.
            transport: The failure transport to read; the default one when
                left out.
            stats: Count the failed messages by class instead.
            class_filter: Only the messages of this class — ``module.QualName``.
            failed_after: Only the messages that failed at or after this date.
            failed_before: Only the messages that failed at or before this date.
        """
        try:
            selected = failed_message_filter(
                class_filter, failed_after, failed_before, with_ids=message_id is not None
            )
            name = self._transport(io, transport)
            await self._print_pending(io, name)
            if stats:
                await self._count_by_class(io, name, selected, max_messages)
            elif message_id is None:
                await self._list(io, name, selected, max_messages or _DEFAULT_MAX)
            else:
                return await self._show(io, name, message_id)
        except (MessageBusError, InvalidInputError) as error:
            io.error(escape(str(error)))
            return ExitCode.FAILURE
        return ExitCode.SUCCESS

    async def _list(
        self, io: ConsoleStyle, name: str | None, selected: FailedMessageFilter, limit: int
    ) -> None:
        if selected.class_name is not None:
            io.text(f"Displaying only '{escape(selected.class_name)}' messages.")
        if selected.failed_after is not None:
            shown = moment_text(selected.failed_after)
            io.text(f"Displaying only messages that failed after {shown}.")
        if selected.failed_before is not None:
            shown = moment_text(selected.failed_before)
            io.text(f"Displaying only messages that failed before {shown}.")
        rows: list[list[str]] = []
        async for envelope in self._failed.all(name, selected, limit):
            sent = envelope.last(SentToFailureTransportStamp)
            error = envelope.last(ErrorDetailsStamp)
            rows.append(
                [
                    escape(FailedMessageRepository.message_id(envelope) or ""),
                    escape(class_name(envelope.message)),
                    sent.failed_at.strftime("%Y-%m-%d %H:%M:%S") if sent is not None else "",
                    escape(error.exception_message) if error is not None else "",
                ]
            )
        if not rows:
            io.success("No failed messages were found.")
            return
        io.table(["Id", "Class", "Failed at", "Error"], rows)
        if len(rows) == limit:
            io.text(f"Showing first {limit} messages.")
        elif not selected.is_empty():
            io.text(f"Showing {len(rows)} message(s).")
        shown = escape(name or "")
        io.text(f"Run messenger:failed:show {{id}} --transport={shown} -vv to see message details.")

    async def _count_by_class(
        self, io: ConsoleStyle, name: str | None, selected: FailedMessageFilter, limit: int | None
    ) -> None:
        counted = Counter(
            [class_name(e.message) async for e in self._failed.all(name, selected, limit)]
        )
        if not counted:
            io.success("No failed messages were found.")
            return
        io.table(["Class", "Count"], [[escape(c), str(n)] for c, n in counted.items()])

    async def _show(self, io: ConsoleStyle, name: str | None, message_id: str) -> int:
        envelope = await self._failed.find(message_id, name)
        if envelope is None:
            io.error(f'The message "{escape(message_id)}" was not found.')
            return ExitCode.FAILURE
        self._display(io, envelope)
        shown, transport = escape(message_id), escape(name or "")
        io.text(
            f"Run messenger:failed:retry {shown} --transport={transport} to retry this message."
        )
        io.text(f"Run messenger:failed:remove {shown} --transport={transport} to delete it.")
        return ExitCode.SUCCESS
