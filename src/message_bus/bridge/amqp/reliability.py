"""How hard to try, and where a message goes when trying stops."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Final

from message_bus.transport.transport_options import as_float, as_int

if TYPE_CHECKING:
    from collections.abc import Mapping

__all__ = ["DEFAULT_DEAD_LETTER_QUEUE", "Reliability"]

DEFAULT_DEAD_LETTER_QUEUE: Final = "taskiq.dlq"

#: Settings :meth:`Reliability.from_settings` reads.
RELIABILITY_OPTIONS: Final = ("max_attempts", "base_delay_seconds", "dead_letter_queue")


@dataclass(frozen=True, slots=True)
class Reliability:
    """How hard to try, and where a message goes when trying stops.

    Attributes:
        max_attempts: Total deliveries including the first, not retries on
            top of it.
        base_delay_seconds: Delay before the first retry; later retries back
            off from it.
        dead_letter_queue: Where a message is republished once attempts run
            out. Set to ``None`` to drop exhausted messages instead.
    """

    max_attempts: int = 3
    base_delay_seconds: float = 1.0
    dead_letter_queue: str | None = DEFAULT_DEAD_LETTER_QUEUE

    @classmethod
    def from_settings(
        cls,
        settings: Mapping[str, str],
        defaults: Reliability | None = None,
    ) -> Reliability:
        """Read reliability out of a transport's settings.

        Anything the settings do not mention keeps its value from
        ``defaults``, so a factory constructed with a policy in code can
        still have one setting overridden per transport.

        Raises:
            InvalidTransportOptionError: If a value is present but not a number.
        """
        base = defaults if defaults is not None else cls()
        dead_letter = settings.get("dead_letter_queue", base.dead_letter_queue)
        return cls(
            max_attempts=as_int(settings, "max_attempts", base.max_attempts),
            base_delay_seconds=as_float(settings, "base_delay_seconds", base.base_delay_seconds),
            dead_letter_queue=dead_letter or None,
        )
