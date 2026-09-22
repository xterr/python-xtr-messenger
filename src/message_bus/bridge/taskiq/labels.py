"""taskiq label keys this adapter reads and writes.

``_retries``, ``delay``, ``priority`` and ``queue_name`` are taskiq's own
label names; the rest belong to this library and are prefixed so they cannot
collide with a label an application sets.
"""

from __future__ import annotations

from typing import Final

__all__ = ["HEADERS_LABEL", "QUEUE_LABEL", "RETRIES_LABEL"]

RETRIES_LABEL: Final = "_retries"
QUEUE_LABEL: Final = "queue_name"
HEADERS_LABEL: Final = "mb_headers"
