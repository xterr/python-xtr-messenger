"""Container-provided dependencies for handlers, via wireup.

Install with the ``wireup`` extra. Two things to write, both from here::

    from wireup import Injected  # wireup's

    from message_bus import as_message_handler
    from message_bus.integration.wireup import takes_injected  # this module


    @as_message_handler(IngestDocument)
    @takes_injected
    async def ingest(message: IngestDocument, db: Injected[Session]) -> None:
        await db.record(message.document_id)

``Injected[T]`` is wireup's annotation, marking a parameter it should fill.
:func:`takes_injected` is this module's decorator, saying the handler has
some. They are separate things and both are needed.

And the wiring, where the container is built::

    from message_bus.integration.wireup import make_injectables

    container = wireup.create_async_container(
        injectables=[app.handlers, *make_injectables(CONFIG, transports=["jobs"])],
    )

    worker = await container.get(WorkerInterface)
    await worker.run()

That is the whole surface: one decorator and one wiring call.
:func:`takes_injected` runs at import, where it can only hide the parameters
— the container does not exist yet, and the bus would otherwise reject a
handler for declaring a parameter it cannot supply.
:func:`make_injectables` runs where the container does exist, and fills them.

wireup opens a scope around each call on its own, so a dependency declared
``lifetime="scoped"`` is built once per message and released when that
message finishes. A singleton stays shared by every message. Scoping
describes what a message should not share, not what a handler must be.

.. note::
   Every annotation a container reads is imported at runtime, not deferred
   into a ``TYPE_CHECKING`` block. wireup resolves a factory's parameters and
   return type when the container is built, and a deferred name is not there
   to resolve.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import TypeVar, final

import wireup
from typing_extensions import override
from wireup import AsyncContainer
from wireup.ioc.util import hide_annotated_names

from message_bus.handler import HandlerDescriptor, HandlersLocatorInterface, default_registry
from message_bus.message_bus_config import MessageBusConfig
from message_bus.message_bus_factory import MessageBusFactory
from message_bus.message_bus_interface import MessageBusInterface
from message_bus.transport.transport_factory_interface import TransportFactoryInterface
from message_bus.worker_factory import WorkerFactory
from message_bus.worker_interface import WorkerInterface

HandlerT = TypeVar("HandlerT")

__all__ = ["make_injectables", "takes_injected"]


def takes_injected(handler: HandlerT) -> HandlerT:
    """Declare that ``handler`` has parameters the container should fill.

    Mark the parameters themselves with wireup's ``Injected[T]``; this says
    the handler has some. Both are needed, and they are different things::

        @as_message_handler(IngestDocument)
        @takes_injected
        async def ingest(message: IngestDocument, db: Injected[Session]) -> None: ...

    What it does is hide those parameters from anything inspecting the
    handler afterwards, which is what lets the bus keep requiring a handler
    to take the message and at most an envelope. Without it, registration
    rejects the handler for declaring a parameter the bus cannot supply.

    Apply it *under* :func:`~message_bus.decorator.as_message_handler`, so
    the shape is already hidden by the time the handler is registered.
    Nothing is resolved here: the container does not exist yet, and
    :func:`make_injectables` supplies it later.
    """
    hide_annotated_names(handler)  # pyright: ignore[reportArgumentType]
    return handler


def make_injectables(
    config: MessageBusConfig | None = None,
    *,
    transports: Sequence[str] = (),
    factories: Sequence[TransportFactoryInterface] | None = None,
    handlers: HandlersLocatorInterface | None = None,
) -> list[object]:
    """Return what a container needs to provide a bus and a worker.

    A container built with these hands out a ``MessageBusInterface``, and a
    ``WorkerInterface`` when ``transports`` names any. Handlers marked with
    :func:`takes_injected` have their parameters filled from the same
    container.

    **The configuration goes in the container either way.** Pass it here and
    it is registered for you; leave it out and provide it yourself, which is
    what you want when it is read from somewhere::

        @injectable
        def bus_config(url: Annotated[str, Inject(config="amqp_url")]) -> MessageBusConfig:
            return MessageBusConfig(transports={"jobs": TransportConfig(url)})

    Either way anything else can ask for a ``MessageBusConfig`` and get the
    same one, and a test can override it like any other injectable.

    Args:
        config: The transports that exist and where messages go. Omit to
            provide it as an injectable of your own.
        transports: What a worker should consume. Omit in a publishing
            process and no worker is registered.
        factories: Transport factories, when discovery is not wanted or a
            factory needs a collaborator it cannot be discovered with.
        handlers: A locator, if not the process-wide one that
            :func:`~message_bus.decorator.as_message_handler` fills.

    Returns:
        Injectables to spread into ``create_async_container(injectables=...)``.
    """

    def filled(container: AsyncContainer) -> HandlersLocatorInterface:
        return _FilledHandlers(container, handlers or default_registry())

    def message_bus(config: MessageBusConfig, container: AsyncContainer) -> MessageBusInterface:
        return MessageBusFactory(config, factories, filled(container)).bus()

    def worker(config: MessageBusConfig, container: AsyncContainer) -> WorkerInterface:
        return WorkerFactory(config, factories, filled(container)).worker(transports)

    registered: list[object] = [wireup.injectable(message_bus)]
    if config is not None:
        registered.append(wireup.instance(config, as_type=MessageBusConfig))
    if transports:
        registered.append(wireup.injectable(worker))
    return registered


@final
class _FilledHandlers(HandlersLocatorInterface):
    """Another locator, with the container filling in handler parameters.

    A decorator rather than a replacement: declaration stays where it was,
    and a handler that asks the container for nothing passes through
    untouched.
    """

    __slots__ = ("_container", "_filled", "_inner")

    def __init__(self, container: AsyncContainer, inner: HandlersLocatorInterface) -> None:
        """Fill handlers from ``inner`` using ``container``."""
        self._container = container
        self._inner = inner
        self._filled: dict[int, HandlerDescriptor] = {}

    @override
    def register(
        self,
        message_type: type,
        handler: object,
        name: str | None = None,
    ) -> HandlerDescriptor:
        """Register on the wrapped locator."""
        return self._inner.register(message_type, handler, name)  # pyright: ignore[reportArgumentType]

    @override
    def handlers_for(self, message_type: type) -> tuple[HandlerDescriptor, ...]:
        """Return the wrapped locator's handlers, each able to be filled."""
        return tuple(self._fill(d) for d in self._inner.handlers_for(message_type))

    @override
    def message_types(self) -> tuple[type, ...]:
        """Return every message type the wrapped locator knows."""
        return self._inner.message_types()

    @override
    def bindings(self) -> tuple[tuple[type, HandlerDescriptor], ...]:
        """Return every ``(message_type, handler)`` pair, filled."""
        return tuple((t, self._fill(d)) for t, d in self._inner.bindings())

    def _fill(self, descriptor: HandlerDescriptor) -> HandlerDescriptor:
        known = self._filled.get(id(descriptor.handler))
        if known is not None:
            return known
        fill = wireup.inject_from_container(self._container)
        made = HandlerDescriptor(
            handler=fill(descriptor.handler),
            name=descriptor.name,
            wants_envelope=descriptor.wants_envelope,
        )
        self._filled[id(descriptor.handler)] = made
        return made
