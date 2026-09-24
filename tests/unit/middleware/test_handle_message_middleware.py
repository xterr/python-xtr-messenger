"""Invoking the handlers registered for a message, at the end of the bus."""

from __future__ import annotations

from dataclasses import dataclass
from typing import final

import pytest
from typing_extensions import override

from tests.support.messages import (
    AnalyseDocument,
    IngestDocument,
    UndeclaredMessage,
    ingest_document,
)
from xtr_messenger import (
    Envelope,
    HandledStamp,
    HandleMessageMiddleware,
    HandlersLocator,
    MiddlewareInterface,
    NoHandlerForMessageError,
    StackInterface,
    as_message,
    as_message_handler,
)

pytestmark = pytest.mark.anyio


@as_message(name="test.unit.middleware.handle.job.v1")
@dataclass(frozen=True, slots=True)
class HandleDefaultJob:
    """A message whose only handler lives in the process-wide registry."""


@final
class Terminal(MiddlewareInterface):
    """Records the envelope handed to it — proof the chain continued past here."""

    def __init__(self) -> None:
        self.seen: list[Envelope] = []

    @override
    async def handle(self, envelope: Envelope, stack: StackInterface, /) -> Envelope:
        del stack
        self.seen.append(envelope)
        return envelope


@final
class OneStep(StackInterface):
    """A stack that hands back a single terminal middleware."""

    def __init__(self, terminal: MiddlewareInterface) -> None:
        self._terminal = terminal

    @override
    def next(self) -> MiddlewareInterface:
        return self._terminal


async def test_every_bound_handler_runs_stamped_in_order_and_the_chain_continues() -> None:
    calls: list[str] = []
    registry = HandlersLocator()

    async def first(message: object) -> None:
        del message
        calls.append("first")

    async def second(message: object) -> None:
        del message
        calls.append("second")

    _ = registry.register(IngestDocument, first, name="first")
    _ = registry.register(IngestDocument, second, name="second")
    terminal = Terminal()

    result = await HandleMessageMiddleware(registry).handle(
        Envelope(ingest_document()),
        OneStep(terminal),
    )

    assert calls == ["first", "second"]
    assert result.all(HandledStamp) == (HandledStamp("first"), HandledStamp("second"))
    assert terminal.seen == [result]


async def test_no_handler_raises_listing_the_handled_types_sorted() -> None:
    registry = HandlersLocator()

    async def handle(message: object) -> None:
        del message

    _ = registry.register(IngestDocument, handle)
    _ = registry.register(AnalyseDocument, handle)

    with pytest.raises(NoHandlerForMessageError) as excinfo:
        _ = await HandleMessageMiddleware(registry).handle(
            Envelope(UndeclaredMessage("x")),
            OneStep(Terminal()),
        )

    assert excinfo.value.message_type is UndeclaredMessage
    assert excinfo.value.handled_types == ("AnalyseDocument", "IngestDocument")


async def test_require_handler_false_passes_an_unhandled_message_quietly() -> None:
    terminal = Terminal()

    result = await HandleMessageMiddleware(HandlersLocator(), require_handler=False).handle(
        Envelope(UndeclaredMessage("x")),
        OneStep(terminal),
    )

    assert result.all(HandledStamp) == ()
    assert terminal.seen == [result]


async def test_a_handler_failure_propagates() -> None:
    registry = HandlersLocator()

    async def boom(message: object) -> None:
        del message
        raise RuntimeError("handler exploded")

    _ = registry.register(IngestDocument, boom)

    with pytest.raises(RuntimeError, match="handler exploded"):
        _ = await HandleMessageMiddleware(registry).handle(
            Envelope(ingest_document()),
            OneStep(Terminal()),
        )


async def test_it_defaults_to_the_process_wide_registry() -> None:
    """With no registry given, handling resolves from the default one that
    ``@as_message_handler`` fills."""
    seen: list[object] = []

    @as_message_handler(HandleDefaultJob)
    async def handle(message: HandleDefaultJob) -> None:
        seen.append(message)

    message = HandleDefaultJob()

    result = await HandleMessageMiddleware().handle(Envelope(message), OneStep(Terminal()))

    assert seen == [message]
    assert result.last(HandledStamp) is not None
