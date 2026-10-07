"""taskiq label keys this adapter reads and writes.

``_retries`` (:data:`RETRIES_LABEL`), ``delay`` (:data:`DELAY_LABEL`) and
``queue_name`` (:data:`QUEUE_LABEL`) are taskiq's own label names, read by
taskiq itself. ``mb_headers`` (:data:`HEADERS_LABEL`) belongs to this library
and is prefixed so it cannot collide with a label an application sets.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Final

if TYPE_CHECKING:
    from collections.abc import Mapping

__all__ = ["DELAY_LABEL", "HEADERS_LABEL", "QUEUE_LABEL", "RETRIES_LABEL", "retries_from"]

RETRIES_LABEL: Final = "_retries"
QUEUE_LABEL: Final = "queue_name"
HEADERS_LABEL: Final = "mb_headers"
DELAY_LABEL: Final = "delay"


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
