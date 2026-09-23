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

Two ways in, depending on who builds the bus. :func:`setup` wires the
container into handlers and leaves you to build a bus the ordinary way.
:func:`make_injectables` has the container hand one out instead.

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

import wireup
from wireup import AsyncContainer

from message_bus.handler import (
    HandlerDescriptor,
    HandlersLocator,
    default_registry,
)
from message_bus.message_bus_config import MessageBusConfig
from message_bus.message_bus_factory import MessageBusFactory
from message_bus.message_bus_interface import MessageBusInterface
from message_bus.transport.transport_factory_interface import TransportFactoryInterface
from message_bus.worker_factory import WorkerFactory
from message_bus.worker_interface import WorkerInterface

__all__ = ["make_injectables", "setup"]

_FILLED = "_message_bus_filled"


def make_injectables(
    config: MessageBusConfig | None = None,
    *,
    transports: Sequence[str] = (),
    factories: Sequence[TransportFactoryInterface] | None = None,
    handlers: HandlersLocator | None = None,
) -> list[object]:
    """Return what a container needs to provide a bus and a worker.

    A container built with these hands out a ``MessageBusInterface``, and a
    ``WorkerInterface`` when ``transports`` names any. Handlers are wired to
    the same container, so :func:`setup` need not be called as well.

    Use this when you would rather ask the container for a bus than build
    one; use :func:`setup` when your application builds it.

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

    def message_bus(config: MessageBusConfig, container: AsyncContainer) -> MessageBusInterface:
        setup(container, handlers)
        return MessageBusFactory(config, factories, handlers).bus()

    def worker(config: MessageBusConfig, container: AsyncContainer) -> WorkerInterface:
        setup(container, handlers)
        return WorkerFactory(config, factories, handlers).worker(transports)

    registered: list[object] = [wireup.injectable(message_bus)]
    if config is not None:
        registered.append(wireup.instance(config, as_type=MessageBusConfig))
    if transports:
        registered.append(wireup.injectable(worker))
    return registered


def setup(container: AsyncContainer, handlers: HandlersLocator | None = None) -> None:
    """Make every declared handler resolve its parameters from ``container``.

    Call once at start-up, after the container is built and after the modules
    declaring handlers have been imported. Then build a bus the ordinary
    way — it needs to know nothing about a container::

        container = wireup.create_async_container(injectables=[app.services])
        setup(container)

        bus = MessageBusFactory(CONFIG).bus()
        worker = WorkerFactory(CONFIG).worker(["jobs"])

    Use this when your application builds the bus. Use
    :func:`make_injectables` instead when you would rather the container hand
    one out.

    There is no configuration argument. A ``MessageBusConfig`` says which
    transports exist and where messages go; a container says what a handler
    can be given. The two have nothing to say to each other, and the bus
    already takes the configuration.

    Calling this twice is harmless — a handler already drawing from a
    container is left alone.

    Args:
        container: The container handler parameters are filled from.
        handlers: The locator to wire, defaulting to the process-wide one.
    """
    registry = handlers if handlers is not None else default_registry()
    registry.decorate(lambda descriptor: _filled(container, descriptor))


def _filled(container: AsyncContainer, descriptor: HandlerDescriptor) -> HandlerDescriptor:
    """Return ``descriptor`` with its container parameters filled in."""
    if getattr(descriptor.handler, _FILLED, False):
        return descriptor
    filled = wireup.inject_from_container(container)(descriptor.handler)
    setattr(filled, _FILLED, True)
    return HandlerDescriptor(
        handler=filled,
        name=descriptor.name,
        wants_envelope=descriptor.wants_envelope,
    )
