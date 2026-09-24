"""The bus, the worker and every handler's dependencies, from a wireup container.

Install with the ``wireup`` extra. Handlers ask for what they need the way
wireup always does, and import nothing from here::

    from wireup import Injected

    from message_bus import as_message_handler


    @as_message_handler(IngestDocument)
    async def ingest(message: IngestDocument, db: Injected[Session]) -> None: ...


    @as_message_handler(IssueInvoice)
    class IssueInvoiceHandler:
        def __init__(self, invoices: InvoiceRepository) -> None: ...

        async def __call__(self, message: IssueInvoice, db: Injected[Session]) -> None: ...

One call where the container is built::

    from message_bus.integration import wireup as message_bus

    container = wireup.create_async_container(
        injectables=[
            app.services,
            *message_bus.injectables(CONFIG, transports=["jobs"]),
        ],
    )

    await (await container.get(WorkerInterface)).run()

and any service can take a ``MessageBusInterface`` like any other dependency.

A handler class is a singleton, registered here — it needs no ``@injectable``
of its own. Its constructor is resolved once, so it takes what lives as long
as it does; anything a single message needs goes on ``__call__`` as
``Injected[...]``, and a ``lifetime="scoped"`` dependency there is built for
that call and released when it finishes, even if it raised. A constructor
asking for a scoped dependency is refused as the container is built.

.. note::
   Every annotation wireup reads is imported at runtime, not deferred into a
   ``TYPE_CHECKING`` block — it resolves them as the container is built.
"""

from __future__ import annotations

import inspect
import types
from collections.abc import Mapping, Sequence
from dataclasses import replace
from typing import cast, final

import wireup
from typing_extensions import override
from wireup import AsyncContainer

from message_bus.exception import UnregisteredHandlerError
from message_bus.handler import Handler, HandlerDescriptor, HandlersLocator, default_registry
from message_bus.message_bus_config import MessageBusConfig
from message_bus.message_bus_factory import MessageBusFactory
from message_bus.message_bus_interface import MessageBusInterface
from message_bus.transport.transport_factory import TransportFactory
from message_bus.transport.transport_factory_interface import TransportFactoryInterface
from message_bus.worker_factory import WorkerFactory
from message_bus.worker_interface import WorkerInterface

__all__ = ["injectables"]


def injectables(
    config: MessageBusConfig | None = None,
    *,
    transports: Sequence[str] = (),
    factories: Sequence[TransportFactoryInterface] | None = None,
    handlers: HandlersLocator | None = None,
) -> list[object]:
    """Return what to spread into ``create_async_container(injectables=[...])``.

    The container then provides a ``MessageBusInterface``, a
    ``WorkerInterface`` when ``transports`` names any, and the
    ``MessageBusConfig`` both are built from — and every handler class
    declared so far, as a singleton. Import the modules declaring handler
    classes before calling this; one declared afterwards is refused with
    :class:`~message_bus.exception.UnregisteredHandlerError` when the bus or
    the worker is resolved.

    Resolving either wires the handlers to the container: a function handler,
    or a handler class's ``__call__``, has its ``Injected[...]`` parameters
    filled on every call.

    Args:
        config: Which transports exist and where messages go. Omit it to
            provide a ``MessageBusConfig`` injectable of your own, say one
            built from ``Inject(config=...)`` settings.
        transports: What this process consumes. Omit in a process that only
            publishes, and no worker is provided.
        factories: Transport factories, when discovery is not wanted or one
            needs a collaborator it cannot be discovered with.
        handlers: The locator handlers were declared into, when not the
            process-wide one.

    Returns:
        Injectables for ``create_async_container``.
    """
    registry = handlers if handlers is not None else default_registry()
    # One for the bus and the worker both, so they share what a factory holds:
    # the connection to a broker, the backlog of an in-memory transport.
    shared = [TransportFactory(factories)]
    registered = {declared: _registration_of(declared) for declared in _handler_classes(registry)}

    def message_bus(bus_config: MessageBusConfig, container: AsyncContainer) -> MessageBusInterface:
        _wire(registry, container, registered)
        return MessageBusFactory(bus_config, shared, registry).bus()

    def worker(bus_config: MessageBusConfig, container: AsyncContainer) -> WorkerInterface:
        _wire(registry, container, registered)
        return WorkerFactory(bus_config, shared, registry).worker(transports)

    provided: list[object] = [wireup.injectable(message_bus)]
    provided.extend(wireup.injectable(registration) for registration in registered.values())
    if config is not None:
        provided.append(wireup.instance(config, as_type=MessageBusConfig))
    if transports:
        provided.append(wireup.injectable(worker))
    return provided


def _handler_classes(registry: HandlersLocator) -> set[type]:
    return {
        descriptor.handler
        for message_type in registry.message_types()
        for descriptor in registry.handlers_for(message_type)
        if isinstance(descriptor.handler, type)
    }


def _registration_of(handler_type: type) -> type:
    """Return what registers ``handler_type`` with the container, as a singleton.

    A private subclass rather than the class itself. ``@injectable`` works by
    marking what it decorates, and a marked handler class would be registered
    a second time by the container scanning the module that declares it.
    Named and placed like the class, so the container's own messages read as
    if they were about it.
    """

    def namespace(body: dict[str, object]) -> None:
        body["__module__"] = handler_type.__module__
        body["__qualname__"] = handler_type.__qualname__

    return types.new_class(handler_type.__name__, (handler_type,), exec_body=namespace)


def _wire(
    registry: HandlersLocator,
    container: AsyncContainer,
    registered: Mapping[type, type],
) -> None:
    """Have every handler draw its dependencies from ``container``.

    Raises:
        WireupError: If a handler asks for something ``container`` cannot
            provide.
        UnregisteredHandlerError: If a handler class was declared after the
            container was built.
    """
    registry.decorate(_ContainerBinding(container, registered))


@final
class _ContainerBinding:
    """Binds each handler to one container, as a locator decorator.

    Every binding is equal to every other, so a locator holds one at a time:
    wiring again — or wiring another container, as a test suite does per
    test — replaces the binding instead of stacking on top of it, and a
    handler declared later is bound to the current container only.
    """

    __slots__ = ("_container", "_registered")

    def __init__(self, container: AsyncContainer, registered: Mapping[type, type]) -> None:
        self._container = container
        self._registered = registered

    def __call__(self, descriptor: HandlerDescriptor) -> HandlerDescriptor:
        return replace(descriptor, call=self._calling(descriptor.handler))

    @override
    def __eq__(self, other: object) -> bool:
        return isinstance(other, _ContainerBinding)

    @override
    def __hash__(self) -> int:
        return hash(_ContainerBinding)

    def _calling(self, declared: Handler | type) -> Handler:
        """Return what to call for ``declared``, with the container filling it in."""
        if isinstance(declared, type):
            return self._shared(declared)
        plain = inspect.isfunction(declared) or inspect.ismethod(declared)
        target: Handler = declared if plain else declared.__call__
        return wireup.inject_from_container(self._container)(target)

    def _shared(self, handler_type: type) -> Handler:
        """Call the container's one ``handler_type``, filling its ``__call__``.

        wireup enters a scope around the call only when ``__call__`` asks for
        something scoped, so a handler needing none costs a lookup per message.

        Raises:
            UnregisteredHandlerError: If ``handler_type`` was declared after
                the container was built.
        """
        registration = self._registered.get(handler_type)
        if registration is None:
            raise UnregisteredHandlerError(handler_type.__qualname__)
        container = self._container
        call = wireup.inject_from_container(container)(cast("Handler", handler_type.__call__))

        async def per_message(*args: object) -> None:
            await call(await container.get(registration), *args)

        return per_message
