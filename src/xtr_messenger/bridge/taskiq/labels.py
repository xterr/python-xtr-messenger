"""taskiq label keys this adapter reads and writes.

``_retries``, ``delay``, ``priority`` and ``queue_name`` are taskiq's own
label names; the rest belong to this library and are prefixed so they cannot
collide with a label an application sets.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Final

if TYPE_CHECKING:
    from collections.abc import Mapping

__all__ = ["HEADERS_LABEL", "QUEUE_LABEL", "RETRIES_LABEL", "retries_from"]

RETRIES_LABEL: Final = "_retries"
QUEUE_LABEL: Final = "queue_name"
HEADERS_LABEL: Final = "mb_headers"


def retries_from(labels: Mapping[str, object]) -> int | None:
    """Return the retry count ``labels`` carry, or ``None`` when it is missing or unreadable.

    Callers decide what an unknown count means; this only reads it, so every
    reader agrees on what counts as known.
    """
    raw = labels.get(RETRIES_LABEL)
    if isinstance(raw, bool) or not isinstance(raw, (int, str)):
        return None
    try:
        return int(raw)
    except ValueError:
        return None
