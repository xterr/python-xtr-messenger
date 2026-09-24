"""Handing routed messages to their transports, and the producer/consumer split."""

from __future__ import annotations

from typing import TYPE_CHECKING, final

import pytest
from typing_extensions import override

from tests.support.fakes import RecordingSender
from tests.support.messages import UndeclaredMessage
from xtr_messenger import (
    Envelope,
    MiddlewareInterface,
    NoSenderForMessageError,
    ReceivedStamp,
    SenderInterface,
    SendersLocatorInterface,
    SendMessageMiddleware,
    SentStamp,
    StackInterface,
)

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator

pytestmark = pytest.mark.anyio


@final
class FakeLocator(SendersLocatorInterface):
    """Yields exactly the ``(alias, sender)`` pairs it was built with."""

    def __init__(
        self,
        pairs: Iterable[tuple[str, SenderInterface]],
        routed: tuple[str, ...] = (),
    ) -> None:
        self._pairs = tuple(pairs)
        self._routed = routed

    @override
    def senders_for(self, envelope: Envelope) -> Iterator[tuple[str, SenderInterface]]:
        del envelope
        yield from self._pairs

    @override
    def routed_type_names(self) -> tuple[str, ...]:
        return self._routed


@final
class HandingBackSender(SenderInterface):
    """Like ``sync://``: marks the envelope received and hands it straight back."""

    def __init__(self) -> None:
        self.sent: list[Envelope] = []

    @override
    async def send(self, envelope: Envelope) -> Envelope:
        self.sent.append(envelope)
        return envelope.with_stamps(ReceivedStamp("here"))


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


async def test_a_routed_message_reaches_its_sender_stamped_with_the_alias() -> None:
    sender = RecordingSender()
    locator = FakeLocator([("async", sender)])

    result = await SendMessageMiddleware(locator).handle(Envelope("payload"), OneStep(Terminal()))

    assert sender.sent[0].last(SentStamp) == SentStamp("RecordingSender", "async")
    assert result.last(SentStamp) == SentStamp("RecordingSender", "async")


async def test_a_fanned_out_message_stamps_every_sender_it_reaches() -> None:
    first, second = RecordingSender(), RecordingSender()
    locator = FakeLocator([("primary", first), ("mirror", second)])

    result = await SendMessageMiddleware(locator).handle(Envelope("payload"), OneStep(Terminal()))

    assert first.sent[0].last(SentStamp) == SentStamp("RecordingSender", "primary")
    assert second.sent[0].last(SentStamp) == SentStamp("RecordingSender", "mirror")
    assert result.all(SentStamp) == (
        SentStamp("RecordingSender", "primary"),
        SentStamp("RecordingSender", "mirror"),
    )


async def test_after_sending_the_chain_short_circuits() -> None:
    """A message handed off to a transport is not also handled in this process."""
    locator = FakeLocator([("async", RecordingSender())])
    terminal = Terminal()

    result = await SendMessageMiddleware(locator).handle(Envelope("payload"), OneStep(terminal))

    assert terminal.seen == []
    assert result.last(SentStamp) is not None


async def test_a_sender_handing_it_back_received_lets_the_chain_continue() -> None:
    """Handing the envelope back received means 'handle it here', as ``sync://`` does."""
    sender = HandingBackSender()
    locator = FakeLocator([("sync", sender)])
    terminal = Terminal()

    result = await SendMessageMiddleware(locator).handle(Envelope("payload"), OneStep(terminal))

    assert terminal.seen[0].last(ReceivedStamp) == ReceivedStamp("here")
    assert result is terminal.seen[0]


async def test_a_received_envelope_is_never_sent_and_continues() -> None:
    """An envelope that arrived from a transport must not be published again."""
    sender = RecordingSender()
    locator = FakeLocator([("async", sender)])
    terminal = Terminal()
    envelope = Envelope("payload").with_stamps(ReceivedStamp("in-memory"))

    result = await SendMessageMiddleware(locator).handle(envelope, OneStep(terminal))

    assert sender.sent == []
    assert terminal.seen == [envelope]
    assert result is envelope


async def test_an_unrouted_message_continues_by_default() -> None:
    locator = FakeLocator([])
    terminal = Terminal()

    result = await SendMessageMiddleware(locator).handle(Envelope("payload"), OneStep(terminal))

    assert terminal.seen == [Envelope("payload")]
    assert result.last(SentStamp) is None


async def test_handle_unrouted_false_stops_at_an_unrouted_message() -> None:
    locator = FakeLocator([])
    terminal = Terminal()
    envelope = Envelope("payload")

    result = await SendMessageMiddleware(locator, handle_unrouted=False).handle(
        envelope,
        OneStep(terminal),
    )

    assert terminal.seen == []
    assert result is envelope


async def test_require_sender_raises_naming_the_routed_types() -> None:
    locator = FakeLocator([], routed=("IngestDocument",))
    envelope = Envelope(UndeclaredMessage("orphan"))

    with pytest.raises(NoSenderForMessageError) as excinfo:
        _ = await SendMessageMiddleware(locator, require_sender=True).handle(
            envelope,
            OneStep(Terminal()),
        )

    assert excinfo.value.message_type is UndeclaredMessage
    assert excinfo.value.routed_types == ("IngestDocument",)
