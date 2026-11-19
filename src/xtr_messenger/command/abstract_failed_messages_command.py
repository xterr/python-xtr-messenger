"""What the ``messenger:failed:*`` commands share: the failure transport, the filter, a message."""

from __future__ import annotations

from typing import TYPE_CHECKING

from xtr_clock import now
from xtr_clock.exception import ClockError
from xtr_console import ConsoleStyle, escape

from xtr_messenger._text import class_name
from xtr_messenger.failure.failed_message_filter import FailedMessageFilter
from xtr_messenger.failure.failed_message_repository import FailedMessageRepository
from xtr_messenger.stamp import (
    ErrorDetailsStamp,
    NonSendableStampInterface,
    RedeliveryStamp,
    SentToFailureTransportStamp,
)

if TYPE_CHECKING:
    from datetime import datetime

    from xtr_messenger.envelope import Envelope

__all__ = [
    "FILTER_OPTIONS",
    "AbstractFailedMessagesCommand",
    "InvalidInputError",
    "failed_message_filter",
    "moment_text",
]

FILTER_OPTIONS = '"--class-filter", "--failed-after" or "--failed-before"'
"""The filter options, as a refusal names them."""


class InvalidInputError(Exception):
    """What the command line asked for cannot be done as asked — said to the operator."""


class AbstractFailedMessagesCommand:
    """Reads the failure transports through a :class:`FailedMessageRepository`.

    A container builds each command with the repository the messenger bundle
    provides, over the failure transports its configuration names.
    """

    __slots__: tuple[str, ...] = ("_failed",)

    def __init__(self, failed: FailedMessageRepository) -> None:
        """Read the failed messages ``failed`` keeps."""
        self._failed: FailedMessageRepository = failed

    def _transport(self, io: ConsoleStyle, transport: str | None) -> str | None:
        """Return the failure transport to read: the one named, else the default, else asked.

        ``None`` only when there is no default and nothing to ask about; the
        repository then says why.
        """
        if transport is not None:
            return transport
        names = self._failed.transport_names
        default = self._failed.global_transport_name
        if default is not None:
            if len(names) > 1:
                others = escape(", ".join(names))
                io.text(
                    f"Loading the global failure transport {escape(default)}; others: {others}."
                )
            return default
        if names and io.interactive:
            return io.ask("Select failed transport:", names[0], choices=names)
        return None

    async def _print_pending(self, io: ConsoleStyle, transport: str | None) -> None:
        pending = await self._failed.count(transport)
        if pending is None:
            return
        if pending == 1:
            io.text("There is [green]1[/] message pending in the failure transport.")
        else:
            io.text(f"There are [green]{pending}[/] messages pending in the failure transport.")

    def _display(self, io: ConsoleStyle, envelope: Envelope) -> None:
        """Write what is known of the failed message ``envelope``."""
        io.title("Failed Message Details")
        rows = [["Class", escape(class_name(envelope.message))]]
        message_id = FailedMessageRepository.message_id(envelope)
        if message_id is not None:
            rows.append(["Message Id", escape(message_id)])
        sent = envelope.last(SentToFailureTransportStamp)
        error = envelope.last(ErrorDetailsStamp)
        if sent is None:
            io.warning("Message does not appear to have been sent to this transport after failing.")
        else:
            rows.extend(
                [
                    ["Failed at", moment_text(sent.failed_at)],
                    ["Error", escape(error.exception_message) if error is not None else ""],
                    [
                        "Error Class",
                        escape(error.exception_class) if error is not None else "(unknown)",
                    ],
                    ["Transport", escape(sent.original_receiver_name)],
                ]
            )
        redelivery = envelope.last(RedeliveryStamp)
        if redelivery is not None:
            rows.append(["Retries", str(redelivery.retry_count)])
        io.table(["Field", "Value"], rows)
        if io.is_very_verbose():
            io.section("Message")
            io.text(escape(repr(envelope.message)))
            io.section("Stamps")
            kept = envelope.without_stamps(NonSendableStampInterface).without_stamps(
                ErrorDetailsStamp
            )
            io.text(escape(repr(list(kept.stamps))) if kept.stamps else "(no data)")
        else:
            io.text("Re-run the command with -vv to see more message details.")


def failed_message_filter(
    class_filter: str | None,
    failed_after: str | None,
    failed_before: str | None,
    *,
    with_ids: bool,
) -> FailedMessageFilter:
    """Return the filter the options ask for.

    Raises:
        InvalidInputError: If a date does not read, or a filter is combined
            with message ids.
    """
    selected = FailedMessageFilter(
        class_name=class_filter,
        failed_after=_date("failed-after", failed_after),
        failed_before=_date("failed-before", failed_before),
    )
    if with_ids and not selected.is_empty():
        raise InvalidInputError(f"Message ids cannot be combined with {FILTER_OPTIONS}.")
    return selected


def _date(option: str, value: str | None) -> datetime | None:
    if value is None:
        return None
    try:
        return now(value)
    except ClockError as error:
        raise InvalidInputError(f'"--{option}" is not a date: {value!r}') from error


def moment_text(moment: datetime) -> str:
    """Return ``moment`` as the failed commands show it: to the second, with its zone."""
    return moment.strftime("%Y-%m-%d %H:%M:%S %Z").strip()
