"""Snapshot of why handling failed."""

from __future__ import annotations

from dataclasses import dataclass

from .stamp_interface import StampInterface

__all__ = ["ErrorDetailsStamp"]


@dataclass(frozen=True, slots=True)
class ErrorDetailsStamp(StampInterface):
    """Snapshot of why handling failed.

    Carries the exception class name and message as plain strings so the
    stamp stays serializable and free of live traceback references.
    """

    exception_class: str
    exception_message: str
