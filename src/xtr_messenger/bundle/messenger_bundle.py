"""The xtr-messenger bundle: a bus and worker factory from a kernel.

An application listing :class:`MessengerBundle` in its ``config/bundles.py`` gets a
:class:`MessageBusInterface`, a :class:`WorkerFactory`, and every handler its
scan finds already wired to the container. The serializer bundle always comes
along: the envelope serializer writes every message through its
``SerializerInterface``. When the console bundle is
active, ``messenger:consume`` is registered too; when the logging bundle is,
the ``"messenger"`` channel is added to its config and the logging
middleware writes through that channel's logger; when the event dispatcher
bundle is, every worker announces itself and its messages through it.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import TYPE_CHECKING, Final, cast, final

from typing_extensions import override
from xtr_dependency_injection import (
    Bundle,
    ContainerBuilder,
    ServiceConfigurator,
    ServiceKey,
    as_bundle,
    bundle_active,
    qualified_name,
    required_bundle,
)
from xtr_serializer.bundle import SerializerBundle

from xtr_messenger.exception import MessageBusError
from xtr_messenger.handler.handler_descriptor import Handler
from xtr_messenger.handler.handlers_locator import HandlersLocator
from xtr_messenger.handler.handlers_registry import handlers_declared_on
from xtr_messenger.handler.redispatch_message_handler import RedispatchMessageHandler
from xtr_messenger.message import RedispatchMessage
from xtr_messenger.message_bus_config import MessageBusConfig
from xtr_messenger.middleware.logging_middleware import LoggingMiddleware
from xtr_messenger.middleware.middleware_interface import MiddlewareInterface
from xtr_messenger.middleware.middleware_registry import middleware_declared_on
from xtr_messenger.transport.receiver.receiver_interface import ReceiverInterface
from xtr_messenger.transport.transport_factory import TransportFactory
from xtr_messenger.transport.transport_factory_interface import TransportFactoryInterface

from ._container_binding import ContainerBinding
from ._middleware_entries import register_middleware_with_arguments
from ._services import (
    MESSENGER_CHANNEL,
    BuiltTransports,
    combined_transport_factory,
    combined_transport_factory_with,
    envelope_serializer,
    logging_middleware_for_messenger,
    message_bus,
    named_middleware,
    worker_factory,
)

if TYPE_CHECKING:
    from collections.abc import Callable

__all__ = ["RECEIVER_TAG", "TRANSPORT_FACTORY_TAG", "MessengerBundle"]

#: Tag a :class:`ReceiverInterface` service with this, and an ``alias``, for
#: workers to consume it by that name beside the configured transports. The
#: worker factory builds every tagged receiver, so building one must do no I/O.
RECEIVER_TAG: Final = "messenger.receiver"

#: Tag a :class:`TransportFactoryInterface` service with this — and alias it
#: under that interface, qualified by the same name — for the bundle to consult
#: it ahead of the factories discovery finds. What another bundle does to serve
#: a scheme with services from the container.
TRANSPORT_FACTORY_TAG: Final = "messenger.transport_factory"

_HANDLES_TAG = "messenger.message_handler"


def _add_messenger_channel(config: object) -> object:
    """Declare the ``messenger`` channel through the logging config's own ``with_channels``.

    Duck-typed: this bundle depends on the logging *contracts* only, never on xtr-logging,
    so it asks the config it is handed rather than importing its type.
    """
    with_channels = cast("Callable[[str], object] | None", getattr(config, "with_channels", None))
    return with_channels(MESSENGER_CHANNEL) if with_channels is not None else config


@final
@required_bundle(SerializerBundle)
@required_bundle("xtr_logging.bundle:LoggingBundle", ignore_on_invalid=True)
@required_bundle("xtr_console.bundle:ConsoleBundle", ignore_on_invalid=True)
@required_bundle("xtr_event_dispatcher.bundle:EventDispatcherBundle", ignore_on_invalid=True)
@as_bundle("messenger", config=MessageBusConfig)
class MessengerBundle(Bundle[MessageBusConfig]):
    """Turns a :class:`MessageBusConfig` into a bus and worker factory in the container."""

    def __init__(self) -> None:
        """Start with an empty per-kernel :class:`HandlersLocator`."""
        self._handlers: HandlersLocator = HandlersLocator()
        self._transports: BuiltTransports = BuiltTransports()

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
            _ = services.set(factory_cls, qualifier=qualifier).add_tag(TRANSPORT_FACTORY_TAG)
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
        """Register the envelope serializer, shared TransportFactory, bus, worker factory, command.

        The redispatch handler is registered here rather than declared, so the
        container builds it with the container's publishing bus — the factories
        then find a handler for :class:`RedispatchMessage` and add none of their own.
        """
        del config
        _ = self._handlers.register(RedispatchMessage, RedispatchMessageHandler)
        _ = services.set(RedispatchMessageHandler).add_tag(
            _HANDLES_TAG, handles=qualified_name(RedispatchMessage)
        )
        _ = services.instance(self._handlers)
        _ = services.instance(self._transports)
        _ = services.set(envelope_serializer)
        _ = services.set(combined_transport_factory)
        _ = services.set(named_middleware)
        if bundle_active(builder, "logging"):
            _ = services.set(
                logging_middleware_for_messenger,
                qualifier="logging",
            )
            services.alias(
                MiddlewareInterface,
                LoggingMiddleware,
                alias_qualifier="logging",
                target_qualifier="logging",
            )
        _ = services.set(message_bus)
        _ = services.set(worker_factory)
        if bundle_active(builder, "console"):
            services.load("xtr_messenger.command")

    @override
    def process(self, builder: ContainerBuilder) -> None:
        """Hand workers the tagged receivers; consult app-registered transport factories first.

        Every service tagged :data:`RECEIVER_TAG` becomes consumable under its
        ``alias`` — aliased under :class:`ReceiverInterface` by that alias here,
        after every bundle and compiler pass before this one has registered what
        it will, so the worker factory's ``ServiceLocator[ReceiverInterface]``
        hands each back keyed by its alias.

        Autoconfiguration tags every class registered under
        :class:`TransportFactoryInterface`. When at least one exists, the shared
        :class:`TransportFactory` is rebuilt from the variant that injects them
        as a ``Sequence[TransportFactoryInterface]``; with none registered the
        discovery-only factory loaded in :meth:`load_extension` stays, since the
        engine cannot resolve an empty collection.
        """
        register_middleware_with_arguments(builder)
        for alias, (service, qualifier) in _receivers(builder).items():
            builder.set_alias(
                ReceiverInterface, service, alias_qualifier=alias, target_qualifier=qualifier
            )
        if not builder.find_tagged_service_ids(TRANSPORT_FACTORY_TAG):
            return
        builder.get_definition(TransportFactory).provider = combined_transport_factory_with

    @override
    async def boot(self) -> None:
        """Bind every declared handler through the container — missing dep fails here."""
        container = self.container
        if container is None:  # pragma: no cover — the kernel sets this before boot.
            message = "MessengerBundle.boot ran without a container"
            raise RuntimeError(message)
        binding = ContainerBinding(container)
        self._handlers.decorate(binding)

    @override
    async def shutdown(self) -> None:
        """Close every connection a publishing transport of *this* kernel opened.

        A producer opens its broker on first publish and nothing closes it
        afterwards — the consuming side's run loop does that, and a publisher
        has no loop. So the transport factory this kernel built is asked to
        release what its own senders opened.

        Scoped to this kernel on purpose. Two applications can run in one
        process, each publishing through its own connections, and closing every
        connection in the process would drop one the other is still using.

        An application that never resolved a transport factory built none, so
        there is nothing to close and no adapter is imported to be asked.
        """
        factory = self._transports.factory
        if factory is None:
            return
        await factory.close_publishers()


def _receivers(builder: ContainerBuilder) -> dict[str, ServiceKey]:
    """Return every tagged receiver's service key, by the alias workers consume it under.

    Raises:
        MessageBusError: If a tagged receiver has no alias, or two share one.
    """
    found: dict[str, ServiceKey] = {}
    for key, tags in builder.find_tagged_service_ids(RECEIVER_TAG).items():
        for attributes in tags:
            alias = attributes.get("alias")
            if not isinstance(alias, str) or not alias:
                raise MessageBusError(f"receiver {key[0].__qualname__} is tagged with no alias")
            if alias in found and found[alias] != key:
                other = found[alias][0].__qualname__
                message = f"receivers {other} and {key[0].__qualname__} share the alias {alias!r}"
                raise MessageBusError(message)
            found[alias] = key
    return found


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
