"""Quarantining poison deliveries instead of holding them unacknowledged.

The wrapped receive loop logs a delivery it cannot parse — or one naming a
task nobody registered — and then simply moves on, leaving it unsettled. On
a broker that redelivers unacknowledged messages, that one delivery comes
back forever, each round logged with its full body. Here such a delivery is
dead-lettered and acknowledged instead, so it can be inspected and replayed
without poisoning the queue it arrived on — and the log names the reason,
never the body, which may carry anything a producer put in a message.
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Final, TypeAlias, final

from taskiq import AckableMessage
from taskiq.receiver import Receiver
from taskiq.utils import maybe_awaitable
from typing_extensions import override

if TYPE_CHECKING:
    from taskiq import AsyncBroker

__all__ = ["DeadLetterer", "PoisonMessageReceiver"]

#: What quarantines a poison delivery: called with the raw body and the
#: reason, it must put the delivery somewhere it can be inspected — or raise,
#: leaving the delivery unsettled for the broker to redeliver.
DeadLetterer: TypeAlias = Callable[[bytes, str], Awaitable[None]]

#: Log lines go through the messenger channel, so an application capturing
#: the standard library's logging sees them beside the rest of the bus's
#: output.
_LOGGER: Final = logging.getLogger("messenger")


@final
class PoisonMessageReceiver(Receiver):
    """The wrapped receive loop, with poison deliveries settled rather than stuck.

    Every delivery is screened before the loop's own handling: one that
    parses and names a registered task goes through untouched. One that does
    not is dead-lettered through ``dead_letter`` and acknowledged — in that
    order, so a failed quarantine leaves the delivery with the broker instead
    of dropping it. With no ``dead_letter`` the delivery is acknowledged and
    dropped, which is what a deployment that configured no dead-letter queue
    asked for.
    """

    def __init__(
        self,
        broker: AsyncBroker,
        *,
        dead_letter: DeadLetterer | None = None,
        max_async_tasks: int | None = None,
        max_prefetch: int = 0,
        run_startup: bool = True,
    ) -> None:
        """Screen ``broker``'s deliveries, quarantining poison through ``dead_letter``."""
        super().__init__(
            broker,
            max_async_tasks=max_async_tasks,
            max_prefetch=max_prefetch,
            run_startup=run_startup,
        )
        self._dead_letter = dead_letter

    @override
    async def callback(
        self,
        message: bytes | AckableMessage,
        raise_err: bool = False,
    ) -> None:
        """Run the loop's own handling, unless the delivery is poison."""
        data = message.data if isinstance(message, AckableMessage) else message
        reason = self._poison_reason(data)
        if reason is None:
            return await super().callback(message, raise_err)
        # The reason only; the body is quarantined, not published to the log.
        _LOGGER.warning("dead-lettering a poison delivery: %s", reason)
        if self._dead_letter is not None:
            await self._dead_letter(data, reason)
        if isinstance(message, AckableMessage):
            await maybe_awaitable(message.ack())
        return None

    def _poison_reason(self, data: bytes) -> str | None:
        """Name why ``data`` can never be handled, or ``None`` when it can be."""
        try:
            parsed = self.broker.formatter.loads(message=data)
            parsed.parse_labels()
        except Exception as error:  # noqa: BLE001 — whatever failed to parse, the delivery is poison
            return f"the delivery could not be parsed ({type(error).__name__})"
        if self.broker.find_task(parsed.task_name) is None:
            return f"no task is registered under {parsed.task_name!r}"
        return None
