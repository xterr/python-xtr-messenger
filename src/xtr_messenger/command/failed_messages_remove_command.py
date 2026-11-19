"""``messenger:failed:remove``: drop messages from a failure transport for good."""

from __future__ import annotations

from typing import Annotated, final

from xtr_console import Argument, ConsoleStyle, ExitCode, Option, as_command, escape

from xtr_messenger.exception import MessageBusError
from xtr_messenger.failure.failed_message_repository import FailedMessageRepository

from .abstract_failed_messages_command import (
    FILTER_OPTIONS,
    AbstractFailedMessagesCommand,
    InvalidInputError,
    failed_message_filter,
)

__all__ = ["FailedMessagesRemoveCommand"]


@as_command("messenger:failed:remove")
@final
class FailedMessagesRemoveCommand(AbstractFailedMessagesCommand):
    """Removes messages from a failure transport — by id, by filter, or every one."""

    async def __call__(  # noqa: PLR0913 — one parameter per argument and option of the command
        self,
        io: ConsoleStyle,
        *message_ids: Annotated[str, Argument(name="id")],
        remove_all: Annotated[bool, Option(name="all")] = False,
        force: bool = False,
        transport: str | None = None,
        show_messages: bool = False,
        class_filter: str | None = None,
        failed_after: str | None = None,
        failed_before: str | None = None,
    ) -> int:
        """Remove given messages from the failure transport.

        The ids come from ``messenger:failed:show``. Instead of ids, messages
        can be selected by class, by when they failed, or both — the filters
        take an ISO-8601 date or a modifier — ``--failed-after='-1 hour'`` — both bounds
        inclusive — and the number found is confirmed before anything goes.

        Args:
            io: Where the command writes.
            message_ids: The messages to remove.
            remove_all: Remove every failed message of the transport.
            force: Remove without asking.
            transport: The failure transport to read; the default one when
                left out.
            show_messages: Show each message before removing it.
            class_filter: Only the messages of this class — ``module.QualName``.
            failed_after: Only the messages that failed at or after this date.
            failed_before: Only the messages that failed at or before this date.
        """
        try:
            selected = failed_message_filter(
                class_filter, failed_after, failed_before, with_ids=bool(message_ids)
            )
            name = self._transport(io, transport)
            if remove_all and not selected.is_empty():
                # Raised to be reported with the other refusals below.
                raise InvalidInputError(f'"--all" cannot be combined with {FILTER_OPTIONS}.')  # noqa: TRY301
            ids = list(message_ids)
            if not selected.is_empty():
                ids = [
                    found
                    async for envelope in self._failed.all(name, selected)
                    if (found := FailedMessageRepository.message_id(envelope)) is not None
                ]
                if not ids:
                    raise InvalidInputError("No failed messages were found with this filter.")  # noqa: TRY301
                plural = "" if len(ids) == 1 else "s"
                if not io.confirm(
                    f"Can you confirm you want to remove {len(ids)} message{plural}?"
                ):
                    return ExitCode.SUCCESS
                force = True
            if not remove_all and not ids:
                raise InvalidInputError(  # noqa: TRY301
                    'Specify at least one message id, or "--all" to remove every failed message.'
                )
            if remove_all and ids:
                raise InvalidInputError('Message ids cannot be combined with "--all".')  # noqa: TRY301
            show = show_messages or len(ids) == 1
            if remove_all:
                await self._remove_all(io, name, force=force, show=show)
            else:
                await self._remove(io, name, ids, force=force, show=show)
        except (MessageBusError, InvalidInputError) as error:
            io.error(escape(str(error)))
            return ExitCode.FAILURE
        return ExitCode.SUCCESS

    async def _remove(
        self, io: ConsoleStyle, name: str | None, ids: list[str], *, force: bool, show: bool
    ) -> None:
        for message_id in ids:
            envelope = await self._failed.find(message_id, name)
            if envelope is None:
                io.error(f'The message with id "{escape(message_id)}" was not found.')
                continue
            if show:
                self._display(io, envelope)
            if force or io.confirm("Do you want to permanently remove this message?"):
                await self._failed.remove(envelope, name)
                io.success(f"Message with id {escape(message_id)} removed.")
            else:
                io.note(f"Message with id {escape(message_id)} not removed.")

    async def _remove_all(
        self, io: ConsoleStyle, name: str | None, *, force: bool, show: bool
    ) -> None:
        if not force:
            pending = await self._failed.count(name)
            question = (
                f"Do you want to permanently remove all ({pending}) messages?"
                if pending is not None
                else "Do you want to permanently remove all failed messages?"
            )
            if not io.confirm(question):
                return
        # Listed first, removed after: removing while listing would page past messages.
        envelopes = [envelope async for envelope in self._failed.all(name)]
        for envelope in envelopes:
            if show:
                self._display(io, envelope)
            await self._failed.remove(envelope, name)
        io.note(f"{len(envelopes)} messages were removed.")
