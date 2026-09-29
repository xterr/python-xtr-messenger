"""The services the messenger bundle has the container build: bus, worker factory, transports.

Each is built from what the container injects, so their annotations are read
at runtime and their types are imported here, not only for type checking.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Annotated, Final, cast, final

from xtr_dependency_injection import (
    ServiceKey,
    ServiceLocator,
    ServicesResetter,
    Target,
    optional_service,
)
from xtr_event_dispatcher_contracts import EventDispatcherInterface
from xtr_logging_contracts import LoggerInterface
from xtr_service_contracts import ContainerInterface

from xtr_messenger.handler.handlers_locator import HandlersLocator
from xtr_messenger.message_bus_config import MessageBusConfig
from xtr_messenger.message_bus_factory import MessageBusFactory
from xtr_messenger.message_bus_interface import MessageBusInterface
from xtr_messenger.middleware.logging_middleware import LoggingMiddleware
from xtr_messenger.middleware.middleware_arguments import entry_arguments, entry_key
from xtr_messenger.middleware.middleware_interface import MiddlewareInterface
from xtr_messenger.middleware.named import MiddlewareBuilder
from xtr_messenger.middleware.unit_of_work_middleware import UnitOfWorkMiddleware
from xtr_messenger.transport.receiver.receiver_interface import ReceiverInterface
from xtr_messenger.transport.transport_factory import TransportFactory
from xtr_messenger.transport.transport_factory_discovery import default_factories
from xtr_messenger.transport.transport_factory_interface import TransportFactoryInterface
from xtr_messenger.worker import AsyncResetter
from xtr_messenger.worker_factory import WorkerFactory

if TYPE_CHECKING:
    from collections.abc import Callable

__all__ = [
    "MESSENGER_CHANNEL",
    "combined_transport_factory",
    "combined_transport_factory_with",
    "logging_middleware_for_messenger",
    "message_bus",
    "named_middleware",
    "worker_factory",
]

MESSENGER_CHANNEL: Final = "messenger"
"""The logging channel the messenger writes through."""


def combined_transport_factory() -> TransportFactory:
    """Build one TransportFactory from every discovered transport factory."""
    return TransportFactory(default_factories())


def combined_transport_factory_with(
    registered: Sequence[TransportFactoryInterface],
) -> TransportFactory:
    """Build one TransportFactory, ``registered`` factories ahead of discovered ones.

    An application — or another bundle — registering a class under
    :class:`TransportFactoryInterface` gets its factory consulted first, in
    registration order, before the entry-point factories discovery finds. The
    bundle swaps this variant in from :meth:`MessengerBundle.process` only when
    at least one such factory exists, because the engine cannot resolve an empty
    ``Sequence[TransportFactoryInterface]``; with none registered the
    discovery-only :func:`combined_transport_factory` is used instead.
    """
    return TransportFactory([*registered, *default_factories()])


def logging_middleware_for_messenger(
    logger: Annotated[LoggerInterface, Target(MESSENGER_CHANNEL)],
) -> LoggingMiddleware:
    """Build :class:`LoggingMiddleware` writing through the ``"messenger"`` channel."""
    return LoggingMiddleware(logger)


@final
@dataclass(frozen=True, slots=True)
class _NamedMiddleware:
    """The named middleware the configuration resolves, built once per kernel.

    Both :func:`message_bus` and :func:`worker_factory` inject this one
    singleton, so the middleware :class:`ServiceLocator` behind
    :func:`_named_from_config` is walked once rather than once per factory.
    """

    builders: Mapping[str, MiddlewareBuilder]


async def named_middleware(
    config: MessageBusConfig, container: ContainerInterface
) -> _NamedMiddleware:
    """Resolve the configuration's named middleware once, as a shared singleton."""
    return _NamedMiddleware(await _named_from_config(config, container))


async def _named_from_config(
    config: MessageBusConfig, container: ContainerInterface
) -> dict[str, Callable[[], MiddlewareInterface]]:
    names = list(_middleware_keys(config))
    locator = ServiceLocator[MiddlewareInterface](
        container, {name: (MiddlewareInterface, name) for name in names}
    )
    built: dict[str, MiddlewareInterface] = {}
    async for name, middleware in locator:
        built[str(name)] = middleware

    def _make_returning(middleware: MiddlewareInterface) -> Callable[[], MiddlewareInterface]:
        return lambda: middleware

    return {name: _make_returning(middleware) for name, middleware in built.items()}


def _middleware_keys(config: MessageBusConfig) -> Iterable[str]:
    """Yield what each named entry of the chain is registered under.

    A bare name is its own key; a name given arguments is a middleware of its
    own, registered by :meth:`MessengerBundle.process` under
    :func:`~xtr_messenger.middleware.middleware_arguments.entry_key`.
    """
    ordinal = 0
    for entry in config.middleware:
        if isinstance(entry, str):
            yield entry
        elif isinstance(entry, Mapping):
            name, _ = entry_arguments(entry)
            yield entry_key(name, ordinal)
            ordinal += 1


def _in_units_of_work(config: MessageBusConfig, container: ContainerInterface) -> MessageBusConfig:
    """Return ``config`` with a unit of work opened around each message, before its middleware."""
    return replace(config, middleware=(UnitOfWorkMiddleware(container), *config.middleware))


def message_bus(
    config: MessageBusConfig,
    handlers: HandlersLocator,
    transports: TransportFactory,
    named: _NamedMiddleware,
    container: ContainerInterface,
) -> MessageBusInterface:
    """Build the bus — its named middleware resolved once, shared with the worker factory.

    Every message dispatched through it is a unit of work.
    """
    return MessageBusFactory(
        _in_units_of_work(config, container), [transports], handlers, named=named.builders
    ).bus()


async def worker_factory(  # noqa: PLR0913, PLR0917 — one parameter per injected service
    config: MessageBusConfig,
    handlers: HandlersLocator,
    transports: TransportFactory,
    named: _NamedMiddleware,
    resetter: ServicesResetter,
    container: ContainerInterface,
    receiver_keys: Mapping[str, ServiceKey],
) -> WorkerFactory:
    """Build the worker factory — every worker resets services after each message.

    Every message a worker handles is a unit of work. Workers announce
    themselves and their messages through the event dispatcher when the
    container has one, and stay silent otherwise. Every tagged receiver is
    built here, consumable under its alias.
    """
    receivers: dict[str, ReceiverInterface] = {}
    for alias, (service, qualifier) in receiver_keys.items():
        receivers[alias] = cast("ReceiverInterface", await container.get(service, qualifier))
    return WorkerFactory(
        _in_units_of_work(config, container),
        [transports],
        handlers,
        named=named.builders,
        resetter=cast("AsyncResetter", resetter),
        event_dispatcher=await optional_service(container, EventDispatcherInterface),
        receivers=receivers,
    )
