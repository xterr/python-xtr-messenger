from __future__ import annotations

import subprocess
import sys
from dataclasses import dataclass
from uuid import UUID, uuid4

import pytest

from message_bus import (
    Envelope,
    HandleMessageMiddleware,
    HandlersLocator,
    MessageBus,
    NoHandlerForMessageError,
    ReceiverInterface,
    SendersLocator,
    SendMessageMiddleware,
    TransportInterface,
    Worker,
    WorkerInterface,
    as_message,
    as_message_handler,
)
from message_bus.stamp import AckReceiptStamp, ErrorDetailsStamp, ReceivedStamp
from message_bus.transport.in_memory.in_memory_transport import InMemoryTransport
from message_bus.transport.sync.sync_transport import SyncTransport

pytestmark = pytest.mark.anyio


@as_message(name="test.work.v1")
@dataclass(frozen=True, slots=True)
class DoWork:
    identifier: UUID
    poison: bool = False


def a_bus(registry: HandlersLocator, transport: InMemoryTransport) -> MessageBus:
    table = SendersLocator({DoWork: "queue"}, {"queue": transport})
    return MessageBus([SendMessageMiddleware(table), HandleMessageMiddleware(registry)])


def a_registry(done: list[UUID]) -> HandlersLocator:
    registry = HandlersLocator()

    @as_message_handler(DoWork, registry=registry)
    async def work(message: DoWork) -> None:
        if message.poison:
            raise ValueError("cannot handle this one")
        done.append(message.identifier)

    return registry


def test_both_shipped_transports_are_whole_transports() -> None:
    """A whole transport composes the send and receive halves."""
    assert isinstance(InMemoryTransport(), TransportInterface)
    assert isinstance(SyncTransport(HandlersLocator()), TransportInterface)
    assert isinstance(InMemoryTransport(), ReceiverInterface)


async def test_a_worker_handles_what_was_published() -> None:
    done: list[UUID] = []
    registry = a_registry(done)
    transport = InMemoryTransport()
    identifier = uuid4()
    _ = await transport.send(Envelope.wrap(DoWork(identifier=identifier)))

    await Worker(a_bus(registry, transport), transport).run()

    assert done == [identifier]


async def test_a_collected_message_is_marked_received_so_it_is_not_republished() -> None:
    transport = InMemoryTransport()
    _ = await transport.send(Envelope.wrap(DoWork(identifier=uuid4())))

    collected = [envelope async for envelope in transport.get()]

    assert collected[0].last(ReceivedStamp) is not None
    assert collected[0].last(AckReceiptStamp) is not None


async def test_one_undeliverable_message_does_not_stop_the_worker() -> None:
    done: list[UUID] = []
    registry = a_registry(done)
    transport = InMemoryTransport()
    first, second = uuid4(), uuid4()
    _ = await transport.send(Envelope.wrap(DoWork(identifier=first)))
    _ = await transport.send(Envelope.wrap(DoWork(identifier=uuid4(), poison=True)))
    _ = await transport.send(Envelope.wrap(DoWork(identifier=second)))

    await Worker(a_bus(registry, transport), transport).run()

    assert done == [first, second]
    assert len(transport.rejected) == 1


async def test_a_rejected_message_carries_why_it_failed() -> None:
    registry = a_registry([])
    transport = InMemoryTransport()
    _ = await transport.send(Envelope.wrap(DoWork(identifier=uuid4(), poison=True)))

    await Worker(a_bus(registry, transport), transport).run()

    details = transport.rejected[0].last(ErrorDetailsStamp)
    assert details is not None
    assert details.exception_class == "ValueError"
    assert details.exception_message == "cannot handle this one"


async def test_draining_the_queue_leaves_the_record_intact() -> None:
    """The assertion surface and the consumable queue are separate on purpose."""
    registry = a_registry([])
    transport = InMemoryTransport()
    for _ in range(3):
        _ = await transport.send(Envelope.wrap(DoWork(identifier=uuid4())))

    await Worker(a_bus(registry, transport), transport).run()

    assert len(transport.sent) == 3
    assert transport.pending == 0


async def test_a_sync_worker_finishes_at_once_because_nothing_is_outstanding() -> None:
    transport = SyncTransport(HandlersLocator())

    collected = [envelope async for envelope in transport.get()]

    assert collected == []


async def test_a_message_with_no_handler_refuses_to_be_acknowledged_quietly() -> None:
    """Silently acking an unhandled message loses the work; an unimported
    handler module is the overwhelmingly likely cause."""
    bus = MessageBus([HandleMessageMiddleware(HandlersLocator())])

    with pytest.raises(NoHandlerForMessageError, match="DoWork"):
        _ = await bus.dispatch(DoWork(identifier=uuid4()))


def test_a_worker_is_recognised_by_its_contract_not_its_class() -> None:
    assert isinstance(Worker(MessageBus([]), InMemoryTransport()), WorkerInterface)


def test_a_full_receive_loop_runs_without_taskiq_installed_in_memory() -> None:
    """The replaceability claim, reduced to something falsifiable.

    If a complete get -> dispatch -> handle -> ack cycle finishes with no
    broker library imported, the receive port demonstrably is not shaped
    around one.
    """
    code = (
        "import asyncio, sys\n"
        "from dataclasses import dataclass\n"
        "from uuid import UUID, uuid4\n"
        "from message_bus import (Envelope, HandleMessageMiddleware, HandlersLocator,\n"
        "    MessageBus, SendersLocator, SendMessageMiddleware, Worker, as_message,\n"
        "    as_message_handler)\n"
        "from message_bus.transport.in_memory.in_memory_transport import InMemoryTransport\n"
        "@as_message(name='probe.v1')\n"
        "@dataclass(frozen=True, slots=True)\n"
        "class Probe:\n"
        "    identifier: UUID\n"
        "seen = []\n"
        "registry = HandlersLocator()\n"
        "@as_message_handler(Probe, registry=registry)\n"
        "async def run(message):\n"
        "    seen.append(message.identifier)\n"
        "async def main():\n"
        "    transport = InMemoryTransport()\n"
        "    await transport.send(Envelope.wrap(Probe(identifier=uuid4())))\n"
        "    table = SendersLocator({Probe: 'q'}, {'q': transport})\n"
        "    bus = MessageBus([SendMessageMiddleware(table),\n"
        "        HandleMessageMiddleware(registry)])\n"
        "    await Worker(bus, transport).run()\n"
        "    assert len(seen) == 1, 'the worker handled nothing'\n"
        "asyncio.run(main())\n"
        "assert 'taskiq' not in sys.modules, 'taskiq was imported'\n"
        "assert 'aio_pika' not in sys.modules, 'aio_pika was imported'\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, check=False
    )

    assert result.returncode == 0, result.stderr
