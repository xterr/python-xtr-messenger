from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

import pytest

from tests.support.fakes import RecordingBus
from tests.support.listing import ListedTransport, ListedTransportFactory
from xtr_messenger import (
    Envelope,
    FailedMessageRepository,
    InMemoryTransportFactory,
    MessageBusConfig,
    NoFailureTransportError,
    ReceivedStamp,
    SentToFailureTransportStamp,
    StampInterface,
    TransportConfig,
    TransportLocator,
    TransportMessageIdStamp,
    UnknownTransportError,
    UnsupportedTransportOperationError,
)

pytestmark = pytest.mark.anyio


def _repository(
    default: str | None = "failed", bus: RecordingBus | None = None
) -> tuple[FailedMessageRepository, ListedTransport]:
    failed = ListedTransport()
    config = MessageBusConfig(
        transports={
            "orders": TransportConfig("in-memory://", failure_transport="failed"),
            "failed": TransportConfig("listed://"),
            "mem": TransportConfig("in-memory://"),
        },
        failure_transport=default,
    )
    locator = TransportLocator(
        config, [ListedTransportFactory({"failed": failed}), InMemoryTransportFactory()]
    )
    names = (*config.failure_transport_names(), "mem")
    repository = FailedMessageRepository(locator, bus or RecordingBus(), names, default)
    return repository, failed


async def test_it_counts_lists_and_finds_in_the_default_failure_transport() -> None:
    repository, failed = _repository()
    message_id = failed.keep(Envelope("x"))

    assert await repository.count() == 1
    assert [e.message async for e in repository.all()] == ["x"]
    found = await repository.find(message_id)
    assert found is not None
    assert repository.message_id(found) == message_id
    assert repository.supports_listing()


async def test_removing_drops_the_message_rather_than_retrying_it() -> None:
    repository, failed = _repository()
    message_id = failed.keep(Envelope("x"))
    found = await repository.find(message_id)
    assert found is not None

    await repository.remove(found)

    assert failed.kept == {}
    assert failed.rejected == []


async def test_redispatching_sends_it_without_what_only_meant_something_where_it_was() -> None:
    bus = RecordingBus()
    repository, failed = _repository(bus=bus)
    kept = Envelope("x", (SentToFailureTransportStamp("orders", datetime.now(UTC)),))
    found = await repository.find(failed.keep(kept))
    assert found is not None

    await repository.redispatch(found.with_stamps(ReceivedStamp("failed")))

    [sent] = bus.dispatched
    assert sent.stamps == ()
    assert failed.kept == {}


async def test_a_failed_redispatch_keeps_the_message() -> None:
    repository, failed = _repository(bus=RecordingBus(RuntimeError("down")))
    found = await repository.find(failed.keep(Envelope("x")))
    assert found is not None

    with pytest.raises(RuntimeError):
        await repository.redispatch(found)

    assert len(failed.kept) == 1


async def test_without_a_default_a_failure_transport_must_be_named() -> None:
    repository, _ = _repository(default=None)

    with pytest.raises(NoFailureTransportError) as raised:
        _ = await repository.count()
    assert raised.value.known == ("failed", "mem")
    assert await repository.count("failed") == 0


async def test_a_transport_that_is_no_failure_transport_is_refused() -> None:
    repository, _ = _repository()

    with pytest.raises(UnknownTransportError):
        _ = await repository.count("orders")


async def test_a_failure_transport_that_cannot_list_is_refused() -> None:
    repository, _ = _repository()

    assert not repository.supports_listing("mem")
    assert await repository.count("mem") is None
    with pytest.raises(UnsupportedTransportOperationError, match="list messages"):
        _ = repository.all("mem")


@dataclass(frozen=True, slots=True)
class _Kept(StampInterface):
    """A stamp of the application's, which a redispatch keeps."""


def test_the_redispatched_envelope_keeps_its_own_stamps() -> None:
    envelope = Envelope("x", (ReceivedStamp("failed"), TransportMessageIdStamp("1"), _Kept()))

    assert FailedMessageRepository.prepare_for_redispatch(envelope).stamps == (_Kept(),)
