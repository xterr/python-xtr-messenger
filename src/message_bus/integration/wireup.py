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

import wireup
from wireup import AsyncContainer

from message_bus.handler import (
    HandlerDescriptor,
    HandlersLocator,
    default_registry,
)

__all__ = ["setup"]

_FILLED = "_message_bus_filled"


def setup(container: AsyncContainer, handlers: HandlersLocator | None = None) -> None:
    """Make every declared handler resolve its parameters from ``container``.

    Call once at start-up, after the container is built and after the modules
    declaring handlers have been imported. Then build a bus the ordinary
    way — it needs to know nothing about a container::

        container = wireup.create_async_container(injectables=[app.services])
        setup(container)

        bus = MessageBusFactory(CONFIG).bus()
        worker = WorkerFactory(CONFIG).worker(["jobs"])

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
