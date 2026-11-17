"""A scoped session, the recorder held-back records end in, and a singleton the resetter resets."""

from __future__ import annotations

from collections.abc import AsyncIterator  # noqa: TC003 — the container reads the return type
from itertools import count
from typing import TYPE_CHECKING, final

from typing_extensions import override
from xtr_dependency_injection import as_service
from xtr_logging import HandlerInterface
from xtr_service_contracts import ResetInterface

if TYPE_CHECKING:
    from collections.abc import Sequence

    from xtr_logging import LogRecord

__all__ = ["RECORDED", "Remembering", "Session", "recorded", "session"]

_NUMBERS = count(1)

RECORDED: list[str] = []
"""Every record that reached the recorder, as ``<message> <value>``."""


@final
class Session:
    """One per message, closed once the message is done with."""

    def __init__(self) -> None:
        self.number = next(_NUMBERS)
        self.closed = False


@as_service(lifetime="scoped")
async def session() -> AsyncIterator[Session]:
    opened = Session()
    try:
        yield opened
    finally:
        opened.closed = True


@final
class _Recorder(HandlerInterface):
    @override
    def is_handling(self, record: LogRecord, /) -> bool:
        return True

    @override
    def handle(self, record: LogRecord, /) -> bool:
        RECORDED.append(_said(record))
        return False

    @override
    def handle_batch(self, records: Sequence[LogRecord], /) -> None:
        RECORDED.extend(_said(record) for record in records)

    @override
    def close(self) -> None:
        pass


def _said(record: LogRecord) -> str:
    return f"{record.message} {record.context.get('value')}"


@as_service(qualifier="recorded")
def recorded() -> HandlerInterface:
    return _Recorder()


@final
@as_service
class Remembering(ResetInterface):
    """A singleton holding what the message under way noted; the resetter clears it after each."""

    def __init__(self) -> None:
        self.noted: list[str] = []
        self.resets = 0

    @override
    async def reset(self) -> None:
        self.noted.clear()
        self.resets += 1
