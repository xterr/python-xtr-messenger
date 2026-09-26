"""The application's message."""

from __future__ import annotations

from dataclasses import dataclass

from xtr_messenger import as_message


@as_message(name="tests.worker_events.ping.v1")
@dataclass(frozen=True)
class Ping:
    """Something for a worker to handle."""

    number: int
