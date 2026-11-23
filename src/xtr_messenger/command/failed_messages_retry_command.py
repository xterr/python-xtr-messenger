"""``messenger:failed:retry``: handle the messages kept in a failure transport again."""

from __future__ import annotations

from typing import TYPE_CHECKING, Annotated, Final, final

from xtr_console import Argument, ConsoleStyle, ExitCode, as_command, escape

from xtr_messenger.exception import InvalidCommandInputError, MessageBusError
from xtr_messenger.failure.failed_message_repository import FailedMessageRepository
from xtr_messenger.transport.receiver.single_message_receiver import SingleMessageReceiver
from xtr_messenger.worker_factory import WorkerFactory

from .abstract_failed_messages_command import (
    AbstractFailedMessagesCommand,
    failed_message_filter,
)

if TYPE_CHECKING:
    from xtr_messenger.envelope import Envelope

__all__ = ["FailedMessagesRetryCommand"]

_RETRY: Final = "retry"
_DELETE: Final = "delete"
_SKIP: Final = "skip"


@as_command("messenger:failed:retry")
@final
class FailedMessagesRetryCommand(AbstractFailedMessagesCommand):
    """Retries the messages kept in a failure transport, one at a time.

    A retried message is handled here, by a worker of its own on the failure
    transport: handled, it is acknowledged and gone; failing again, the
    failure transport does with it what it does with any failed message. It
    is never sent on to another failure transport: it stays where it failed
    for good, and keeps the stamp naming where it first failed.
    """

    def __init__(self, failed: FailedMessageRepository, workers: WorkerFactory) -> None:
        """Read ``failed``, handling each retried message through a worker ``workers`` builds."""
        super().__init__(failed)
        self._workers: WorkerFactory = workers

    async def __call__(  # noqa: PLR0913 — one parameter per argument and option of the command
        self,
        io: ConsoleStyle,
        *message_ids: Annotated[str, Argument(name="id")],
        force: bool = False,
        redispatch: bool = False,
        transport: str | None = None,
        class_filter: str | None = None,
        failed_after: str | None = None,
        failed_before: str | None = None,
    ) -> int:
        """Retry one or more messages from the failure transport.

        Without ids, each failed message is shown and you are asked to
        retry, delete or skip it. Instead of ids, messages can be selected by
        class, by when they failed, or both — the filters take an ISO-8601
        date or a modifier — ``--failed-after='-1 hour'`` — both bounds inclusive.

        Args:
            io: Where the command writes.
            message_ids: The messages to retry.
            force: Retry without asking.
            redispatch: Dispatch each message to its transport again instead
                of handling it here.
            transport: The failure transport to read; the default one when
                left out.
            class_filter: Only the messages of this class — ``module.QualName``.
            failed_after: Only the messages that failed at or after this date.
            failed_before: Only the messages that failed at or before this date.
        """
        try:
            selected = failed_message_filter(
                class_filter, failed_after, failed_before, with_ids=bool(message_ids)
            )
            name = self._transport(io, transport)
            await self._print_pending(io, name)
            io.text(f"To retry all the messages, run messenger:consume {escape(name or '')}.")
            ids = list(message_ids)
            if not selected.is_empty():
                ids = [
                    found
                    async for envelope in self._failed.all(name, selected)
                    if (found := FailedMessageRepository.message_id(envelope)) is not None
                ]
                if not ids:
                    raise InvalidCommandInputError(  # noqa: TRY301
                        "No failed messages were found with this filter."
                    )
            if ids:
                succeeded = await self._retry_ids(io, name, ids, force=force, redispatch=redispatch)
            elif not io.interactive:
                raise InvalidCommandInputError(  # noqa: TRY301
                    "Message ids must be passed when in non-interactive mode."
                )
            else:
                succeeded = await self._retry_interactively(
                    io, name, force=force, redispatch=redispatch
                )
        except (MessageBusError, InvalidCommandInputError) as error:
            io.error(escape(str(error)))
            return ExitCode.FAILURE
        if not succeeded:
            return ExitCode.FAILURE
        io.success("All done!")
        return ExitCode.SUCCESS

    async def _retry_ids(
        self, io: ConsoleStyle, name: str | None, ids: list[str], *, force: bool, redispatch: bool
    ) -> bool:
        succeeded = True
        for message_id in ids:
            envelope = await self._failed.find(message_id, name)
            if envelope is None:
                raise InvalidCommandInputError(f'The message "{message_id}" was not found.')
            succeeded = (
                await self._settle(io, name, envelope, force=force, redispatch=redispatch)
                and succeeded
            )
        return succeeded

    async def _retry_interactively(
        self, io: ConsoleStyle, name: str | None, *, force: bool, redispatch: bool
    ) -> bool:
        """Offer each failed message once, oldest first, until none is left unseen.

        Found one by one rather than received: receiving would hand a message
        out, and it would stay out for as long as this asks about it.
        """
        seen: set[str] = set()
        succeeded = True
        while (envelope := await self._next_unseen(name, seen)) is not None:
            message_id = FailedMessageRepository.message_id(envelope)
            if message_id is not None:
                seen.add(message_id)
            succeeded = (
                await self._settle(io, name, envelope, force=force, redispatch=redispatch)
                and succeeded
            )
        return succeeded

    async def _next_unseen(self, name: str | None, seen: set[str]) -> Envelope | None:
        async for envelope in self._failed.all(name):
            if FailedMessageRepository.message_id(envelope) not in seen:
                return envelope
        return None

    async def _settle(
        self,
        io: ConsoleStyle,
        name: str | None,
        envelope: Envelope,
        *,
        force: bool,
        redispatch: bool,
    ) -> bool:
        """Show ``envelope``, then retry, delete or skip it; ``False`` if not retried as asked."""
        self._display(io, envelope)
        choice = (
            _RETRY
            if force
            else io.ask("Please select an action", _RETRY, choices=[_RETRY, _DELETE, _SKIP])
        )
        if choice == _DELETE:
            await self._failed.remove(envelope, name)
            return True
        if choice == _SKIP:
            return True
        if redispatch:
            try:
                await self._failed.redispatch(envelope, name)
            except MessageBusError as error:
                reason = escape(str(error))
                io.error(f"The message was not redispatched, it stays where it is: {reason}")
                return False
            return True
        receiver = self._failed.receiver(name)
        worker = self._workers.worker_for(name or "", SingleMessageReceiver(receiver, envelope))
        await worker.run()
        return True
