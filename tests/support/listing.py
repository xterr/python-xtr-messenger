"""A transport that can be listed, counted and set up, and an invoker building commands."""

from __future__ import annotations

from typing import TYPE_CHECKING, cast, final, override

from xtr_console import CommandInvokerInterface

from xtr_messenger import (
    Dsn,
    Envelope,
    ListableReceiverInterface,
    MessageCountAwareInterface,
    SetupableTransportInterface,
    TransportConfig,
    TransportFactoryInterface,
    TransportInterface,
    TransportMessageIdStamp,
)

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Awaitable, Callable, Mapping

    from xtr_console.command import CommandArguments, CommandDescriptor, CommandSignature


@final
class ListedTransport(
    TransportInterface,
    ListableReceiverInterface,
    MessageCountAwareInterface,
    SetupableTransportInterface,
):
    """Keeps what is sent until it is acknowledged; rejecting keeps it too, recorded."""

    def __init__(self, *envelopes: Envelope) -> None:
        self.kept: dict[str, Envelope] = {}
        self.rejected: list[Envelope] = []
        self.acknowledged: list[Envelope] = []
        self.set_up = False
        for envelope in envelopes:
            _ = self.keep(envelope)

    def keep(self, envelope: Envelope) -> str:
        message_id = str(len(self.kept) + len(self.acknowledged) + 1)
        self.kept[message_id] = envelope.with_stamps(TransportMessageIdStamp(message_id))
        return message_id

    @override
    async def send(self, envelope: Envelope) -> Envelope:
        return self.kept[self.keep(envelope)]

    @override
    async def get(self) -> AsyncIterator[Envelope]:
        for envelope in list(self.kept.values()):
            yield envelope

    @override
    async def ack(self, envelope: Envelope) -> None:
        stamp = envelope.last(TransportMessageIdStamp)
        assert stamp is not None
        self.acknowledged.append(self.kept.pop(stamp.message_id))

    @override
    async def reject(self, envelope: Envelope) -> None:
        self.rejected.append(envelope)

    @override
    async def all(self, limit: int | None = None) -> AsyncIterator[Envelope]:
        for envelope in list(self.kept.values())[:limit]:
            yield envelope

    @override
    async def find(self, message_id: str) -> Envelope | None:
        return self.kept.get(message_id)

    @override
    async def count(self) -> int:
        return len(self.kept)

    @override
    async def setup(self) -> None:
        self.set_up = True


@final
class ListedTransportFactory(TransportFactoryInterface):
    """Serves ``listed://`` with the transports it was given, by name."""

    def __init__(self, transports: Mapping[str, ListedTransport]) -> None:
        self._transports = transports

    @override
    def supports(self, dsn: Dsn) -> bool:
        return dsn.scheme == "listed"

    @override
    def create(self, group: Mapping[str, TransportConfig]) -> Mapping[str, TransportInterface]:
        return {name: self._transports[name] for name in group}


@final
class BuildingInvoker(CommandInvokerInterface):
    """Builds each command class with ``services``, as a container would."""

    def __init__(self, services: Mapping[type, Callable[[], object]]) -> None:
        self._services = services

    @override
    async def invoke(
        self,
        command: CommandDescriptor,
        signature: CommandSignature,
        arguments: CommandArguments,
    ) -> object:
        del signature
        command_type = cast("type", command.target)
        # The arguments come parsed off the command line, typed only as objects.
        call = cast("Callable[..., Awaitable[object]]", self._services[command_type]())
        return await call(*arguments.args, **arguments.kwargs)
