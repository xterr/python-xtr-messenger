"""Everything wired, for applications using a wireup container.

.. note::
   Every annotation a container reads is imported at runtime, not deferred
   into a ``TYPE_CHECKING`` block. wireup resolves a factory's parameters and
   return type when the container is built, and a deferred name is not there
   to resolve.


Install with the ``wireup`` extra. One call registers the whole graph::

    import wireup
    from message_bus.integration.wireup import make_injectables

    container = wireup.create_async_container(
        injectables=[
            app.handlers,
            *make_injectables(CONFIG, handlers={IngestDocument: IngestHandler}),
        ],
    )

    bus = await container.get(MessageBusInterface)

Handlers are **resolved per message, inside a scope**. A handler declared
``lifetime="scoped"`` — or holding anything that is — gets a fresh instance
for each message, and whatever that instance opened is released when the
message finishes. A database session per message costs nothing to arrange.

Declaring handlers explicitly, rather than with
:func:`~message_bus.decorator.as_message_handler`, is the point: the
decorator writes to a process-wide registry at import time, which a
container cannot reach into. Mixing the two silently declares handlers into
one registry and resolves them from another.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, cast, final

import wireup
from typing_extensions import override
from wireup import AsyncContainer

from message_bus.exception import UnresolvableHandlerError
from message_bus.handler import HandlerDescriptor, HandlersLocatorInterface
from message_bus.message_bus_factory import MessageBusFactory
from message_bus.message_bus_interface import MessageBusInterface
from message_bus.worker_factory import WorkerFactory
from message_bus.worker_interface import WorkerInterface

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from message_bus.envelope import Envelope
    from message_bus.handler import Handler
    from message_bus.message_bus_config import MessageBusConfig
    from message_bus.transport.transport_factory_interface import TransportFactoryInterface

__all__ = ["ContainerHandlersLocator", "make_injectables"]


def make_injectables(
    config: MessageBusConfig,
    *,
    handlers: Mapping[type, type] | None = None,
    transports: Sequence[str] = (),
    factories: Sequence[TransportFactoryInterface] | None = None,
) -> list[object]:
    """Return everything a container needs to provide a bus and a worker.

    Args:
        config: The transports that exist and where messages go.
        handlers: Message type to the class that handles it. Each class is
            resolved from the container when a message arrives, so it may
            take dependencies of its own.
        transports: The transports a worker should consume. Omit in a
            publishing process, and ``WorkerInterface`` is simply not
            registered.
        factories: Transport factories, if discovery is not wanted or a
            factory needs a collaborator it cannot be discovered with.

    Returns:
        Injectables to spread into ``create_async_container(injectables=...)``.
    """
    bound = dict(handlers or {})
    given = list(factories) if factories is not None else None

    def handlers_locator(container: AsyncContainer) -> HandlersLocatorInterface:
        return ContainerHandlersLocator(container, bound)

    def message_bus(registry: HandlersLocatorInterface) -> MessageBusInterface:
        return MessageBusFactory(config, given, registry).bus()

    def worker(registry: HandlersLocatorInterface) -> WorkerInterface:
        return WorkerFactory(config, given, registry).worker(transports)

    registered: list[object] = [
        wireup.injectable(handlers_locator),
        wireup.injectable(message_bus),
    ]
    if transports:
        registered.append(wireup.injectable(worker))
    return registered


@final
class ContainerHandlersLocator(HandlersLocatorInterface):
    """Looks handlers up in a container instead of holding them.

    Registration is a mapping of message type to handler *class*. Nothing is
    built until a message of that type arrives, and it is built inside a
    scope that closes when the message is finished with — so a handler, or
    anything it depends on, can be scoped to one message.
    """

    __slots__ = ("_bound", "_container")

    def __init__(self, container: AsyncContainer, bound: Mapping[type, type]) -> None:
        """Resolve the classes in ``bound`` from ``container``, per message."""
        self._container = container
        self._bound = dict(bound)

    @override
    def register(
        self,
        message_type: type,
        handler: Handler,
        name: str | None = None,
    ) -> HandlerDescriptor:
        """Bind an already-built ``handler``, bypassing the container.

        For a handler that needs nothing injected, or one built by hand.
        """
        del name
        descriptor = HandlerDescriptor.of(handler)
        self._bound[message_type] = type(handler)
        return descriptor

    @override
    def handlers_for(self, message_type: type) -> tuple[HandlerDescriptor, ...]:
        """Return a descriptor per bound class, most specific first."""
        found: list[HandlerDescriptor] = []
        seen: set[type] = set()
        for base in message_type.__mro__:
            handler_type = self._bound.get(base)
            if handler_type is None or handler_type in seen:
                continue
            seen.add(handler_type)
            found.append(self._descriptor_for(handler_type))
        return tuple(found)

    @override
    def message_types(self) -> tuple[type, ...]:
        """Return every message type with a handler bound."""
        return tuple(self._bound)

    @override
    def bindings(self) -> tuple[tuple[type, HandlerDescriptor], ...]:
        """Return every ``(message_type, handler)`` pair, for wiring."""
        return tuple(
            (message_type, self._descriptor_for(handler_type))
            for message_type, handler_type in self._bound.items()
        )

    def _descriptor_for(self, handler_type: type) -> HandlerDescriptor:
        async def resolve_and_call(message: object, envelope: Envelope | None = None) -> None:
            async with self._container.enter_scope() as scope:
                # basedpyright reads scope.get as returning Any and wants this
                # narrowed; ty resolves it and calls the cast redundant.
                built = cast("object", await scope.get(handler_type))  # ty: ignore[redundant-cast]
                if built is None:
                    raise UnresolvableHandlerError(handler_type)
                handler = cast("Handler", built)
                await (handler(message) if envelope is None else handler(message, envelope))

        return HandlerDescriptor.of_type(handler_type, resolve_and_call)
