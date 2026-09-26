"""The xtr-messenger bundle: a bus and worker factory from a kernel.

An application listing :class:`MessengerBundle` in its ``bundles.py`` gets a
:class:`MessageBusInterface`, a :class:`WorkerFactory`, and every handler its
scan finds already wired to the container. When the console bundle is
active, ``messenger:consume`` is registered too; when the logging bundle is,
the ``"messenger"`` channel is added to its config and the logging
middleware writes through that channel's logger.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Annotated, cast, final

from typing_extensions import override
from xtr_dependency_injection import (
    Bundle,
    ContainerBuilder,
    ServiceConfigurator,
    ServiceLocator,
    ServicesResetter,
    Target,
    as_bundle,
    bind_callable,
    bundle_active,
    qualified_name,
    required_bundle,
)
from xtr_logging_contracts import LoggerInterface
from xtr_service_contracts import ContainerInterface

from xtr_messenger.handler.handler_descriptor import Handler, HandlerDescriptor
from xtr_messenger.handler.handlers_locator import HandlersLocator
from xtr_messenger.handler.handlers_registry import handlers_declared_on
from xtr_messenger.message_bus_config import MessageBusConfig
from xtr_messenger.message_bus_factory import MessageBusFactory
from xtr_messenger.message_bus_interface import MessageBusInterface
from xtr_messenger.middleware.logging_middleware import LoggingMiddleware
from xtr_messenger.middleware.middleware_interface import MiddlewareInterface
from xtr_messenger.middleware.middleware_registry import middleware_declared_on
from xtr_messenger.middleware.named import MiddlewareBuilder
from xtr_messenger.transport.transport_factory import TransportFactory
from xtr_messenger.transport.transport_factory_discovery import default_factories
from xtr_messenger.transport.transport_factory_interface import TransportFactoryInterface
from xtr_messenger.worker import AsyncResetter
from xtr_messenger.worker_factory import WorkerFactory

if TYPE_CHECKING:
    from collections.abc import Callable

__all__ = ["MessengerBundle"]

_HANDLES_TAG = "messenger.message_handler"
_TRANSPORT_FACTORY_TAG = "messenger.transport_factory"
_MESSENGER_CHANNEL = "messenger"


def _add_messenger_channel(config: object) -> object:
    """Declare the ``messenger`` channel through the logging config's own ``with_channels``.

    Duck-typed: this bundle depends on the logging *contracts* only, never on xtr-logging,
    so it asks the config it is handed rather than importing its type.
    """
    with_channels = cast("Callable[[str], object] | None", getattr(config, "with_channels", None))
    return with_channels(_MESSENGER_CHANNEL) if with_channels is not None else config


def _combined_transport_factory() -> TransportFactory:
    """Build one TransportFactory from every discovered transport factory."""
    return TransportFactory(default_factories())


def _combined_transport_factory_with(
    registered: Sequence[TransportFactoryInterface],
) -> TransportFactory:
    """Build one TransportFactory, ``registered`` factories ahead of discovered ones.

    An application — or another bundle — registering a class under
    :class:`TransportFactoryInterface` gets its factory consulted first, in
    registration order, before the entry-point factories discovery finds. The
    bundle swaps this variant in from :meth:`MessengerBundle.process` only when
    at least one such factory exists, because the engine cannot resolve an empty
    ``Sequence[TransportFactoryInterface]``; with none registered the
    discovery-only :func:`_combined_transport_factory` is used instead.
    """
    return TransportFactory([*registered, *default_factories()])


def _logging_middleware_for_messenger(
    logger: Annotated[LoggerInterface, Target(_MESSENGER_CHANNEL)],
) -> LoggingMiddleware:
    """Build :class:`LoggingMiddleware` writing through the ``"messenger"`` channel."""
    return LoggingMiddleware(logger)


@final
@dataclass(frozen=True, slots=True)
class _NamedMiddleware:
    """The named middleware the configuration resolves, built once per kernel.

    Both :func:`_message_bus` and :func:`_worker_factory` inject this one
    singleton, so the middleware :class:`ServiceLocator` behind
    :func:`_named_from_config` is walked once rather than once per factory.
    """

    builders: Mapping[str, MiddlewareBuilder]


async def _named_middleware(
    config: MessageBusConfig, container: ContainerInterface
) -> _NamedMiddleware:
    """Resolve the configuration's named middleware once, as a shared singleton."""
    return _NamedMiddleware(await _named_from_config(config, container))


async def _named_from_config(
    config: MessageBusConfig, container: ContainerInterface
) -> dict[str, Callable[[], MiddlewareInterface]]:
    names = [entry for entry in config.middleware if isinstance(entry, str)]
    locator = ServiceLocator[MiddlewareInterface](
        container, {name: (MiddlewareInterface, name) for name in names}
    )
    built: dict[str, MiddlewareInterface] = {}
    async for name, middleware in locator:
        built[str(name)] = middleware

    def _make_returning(middleware: MiddlewareInterface) -> Callable[[], MiddlewareInterface]:
        return lambda: middleware

    return {name: _make_returning(middleware) for name, middleware in built.items()}


@final
@required_bundle("xtr_logging.bundle:LoggingBundle", ignore_on_invalid=True)
@required_bundle("xtr_console.bundle:ConsoleBundle", ignore_on_invalid=True)
@as_bundle("messenger", config=MessageBusConfig)
class MessengerBundle(Bundle[MessageBusConfig]):
    """Turns a :class:`MessageBusConfig` into a bus and worker factory in the container."""

    def __init__(self) -> None:
        """Start with an empty per-kernel :class:`HandlersLocator`."""
        self._handlers: HandlersLocator = HandlersLocator()

    @override
    def prepend_extension(self, builder: ContainerBuilder) -> None:
        """When ``logging`` is active, add the ``messenger`` channel to its config."""
        if not bundle_active(builder, "logging"):
            return
        builder.prepend_extension_config("logging", _add_messenger_channel)

    @override
    def build(self, builder: ContainerBuilder) -> None:
        """Autoconfigure declared handlers, named middleware and transport factories."""
        handlers = self._handlers

        def register_handler(
            obj: object, message_type: type, services: ServiceConfigurator
        ) -> None:
            handler_target = cast("Handler | type", obj)
            _ = handlers.register(message_type, handler_target)
            if isinstance(obj, type):
                _ = services.set(obj).add_tag(_HANDLES_TAG, handles=qualified_name(message_type))

        def register_middleware(obj: object, name: str, services: ServiceConfigurator) -> None:
            if not isinstance(obj, type):
                return
            middleware_cls = cast("type[MiddlewareInterface]", obj)
            _ = services.set(middleware_cls, qualifier=name)
            services.alias(
                MiddlewareInterface,
                middleware_cls,
                alias_qualifier=name,
                target_qualifier=name,
            )

        def register_transport_factory(
            obj: object, factory_cls: type, services: ServiceConfigurator
        ) -> None:
            del obj
            qualifier = qualified_name(factory_cls)
            _ = services.set(factory_cls, qualifier=qualifier).add_tag(_TRANSPORT_FACTORY_TAG)
            services.alias(
                TransportFactoryInterface,
                factory_cls,
                alias_qualifier=qualifier,
                target_qualifier=qualifier,
            )

        builder.register_attribute_for_autoconfiguration(handlers_declared_on, register_handler)
        builder.register_attribute_for_autoconfiguration(
            middleware_declared_on, register_middleware
        )
        builder.register_attribute_for_autoconfiguration(
            _transport_factories_in, register_transport_factory
        )

    @override
    def load_extension(
        self,
        config: MessageBusConfig,
        services: ServiceConfigurator,
        builder: ContainerBuilder,
    ) -> None:
        """Register the shared TransportFactory, bus, worker factory, and command."""
        del config
        _ = services.instance(self._handlers)
        _ = services.set(_combined_transport_factory)
        _ = services.set(_named_middleware)
        if bundle_active(builder, "logging"):
            _ = services.set(
                _logging_middleware_for_messenger,
                qualifier="logging",
            )
            services.alias(
                MiddlewareInterface,
                LoggingMiddleware,
                alias_qualifier="logging",
                target_qualifier="logging",
            )
        _ = services.set(_message_bus)
        _ = services.set(_worker_factory)
        if bundle_active(builder, "console"):
            services.load("xtr_messenger.command")

    @override
    def process(self, builder: ContainerBuilder) -> None:
        """Consult app-registered transport factories ahead of discovered ones.

        Autoconfiguration tags every class registered under
        :class:`TransportFactoryInterface`. When at least one exists, the shared
        :class:`TransportFactory` is rebuilt from the variant that injects them
        as a ``Sequence[TransportFactoryInterface]``; with none registered the
        discovery-only factory loaded in :meth:`load_extension` stays, since the
        engine cannot resolve an empty collection.
        """
        if not builder.find_tagged_service_ids(_TRANSPORT_FACTORY_TAG):
            return
        builder.get_definition(TransportFactory).provider = _combined_transport_factory_with

    @override
    async def boot(self) -> None:
        """Bind every declared handler through the container — missing dep fails here."""
        container = self.container
        if container is None:  # pragma: no cover — the kernel sets this before boot.
            message = "MessengerBundle.boot ran without a container"
            raise RuntimeError(message)
        binding = _ContainerBinding(container)
        self._handlers.decorate(binding)


def _transport_factories_in(obj: object) -> Iterable[type]:
    """Yield ``obj`` when it is a concrete :class:`TransportFactoryInterface`."""
    if not isinstance(obj, type):
        return ()
    if obj is TransportFactoryInterface or obj is TransportFactory:
        return ()
    try:
        matches = issubclass(obj, TransportFactoryInterface)
    except TypeError:
        return ()
    return (obj,) if matches else ()


def _message_bus(
    config: MessageBusConfig,
    handlers: HandlersLocator,
    transports: TransportFactory,
    named: _NamedMiddleware,
) -> MessageBusInterface:
    """Build the bus — its named middleware resolved once, shared with the worker factory."""
    return MessageBusFactory(config, [transports], handlers, named=named.builders).bus()


def _worker_factory(
    config: MessageBusConfig,
    handlers: HandlersLocator,
    transports: TransportFactory,
    named: _NamedMiddleware,
    resetter: ServicesResetter,
) -> WorkerFactory:
    """Build the worker factory — every worker resets services after each message."""
    return WorkerFactory(
        config,
        [transports],
        handlers,
        named=named.builders,
        resetter=cast("AsyncResetter", resetter),
    )


@final
class _ContainerBinding:
    """Rebinds every :class:`HandlerDescriptor.call` through a container.

    Two instances are equal so :meth:`HandlersLocator.decorate` replaces an
    earlier binding instead of stacking on it — a handler declared after the
    kernel booted is bound to the current container only.
    """

    __slots__ = ("_container",)

    def __init__(self, container: ContainerInterface) -> None:
        self._container = container

    def __call__(self, descriptor: HandlerDescriptor) -> HandlerDescriptor:
        target = descriptor.handler
        bound = bind_callable(self._container, target)
        return replace(descriptor, call=bound)

    @override
    def __eq__(self, other: object) -> bool:
        return isinstance(other, _ContainerBinding)

    @override
    def __hash__(self) -> int:
        return hash(_ContainerBinding)
