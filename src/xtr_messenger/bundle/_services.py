"""The services the messenger bundle has the container build: bus, worker factory, transports.

Each is built from what the container injects, so their annotations are read
at runtime and their types are imported here, not only for type checking.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Annotated, Final, cast, final

from xtr_dependency_injection import (
    ScopeFactoryInterface,
    ServiceLocator,
    ServicesResetter,
    Target,
)
from xtr_event_dispatcher_contracts import EventDispatcherInterface
from xtr_logging_contracts import LoggerInterface

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
from xtr_messenger.transport.transport_factory_interface import TransportFactoryInterface
from xtr_messenger.worker import AsyncResetter
from xtr_messenger.worker_factory import WorkerFactory

if TYPE_CHECKING:
    from collections.abc import Callable

__all__ = [
    "MESSENGER_CHANNEL",
    "BuiltTransports",
    "combined_transport_factory",
    "combined_transport_factory_with",
    "logging_middleware_for_messenger",
    "message_bus",
    "named_middleware",
    "worker_factory",
]

MESSENGER_CHANNEL: Final = "messenger"
"""The logging channel the messenger writes through."""


@final
class BuiltTransports:
    """Where one kernel's :class:`TransportFactory` is recorded as it is built.

    :meth:`~xtr_messenger.bundle.MessengerBundle.shutdown` has to close the
    publish connections its own kernel opened, and only those: another kernel in
    the same process may still be publishing through its own. A container cannot
    be asked whether it ever built a service, and building one at shutdown just
    to close it would import every advertised adapter in an application that
    never sent a message — so the factory functions below record what they built
    here, on an instance the bundle owns.

    Mutable, and empty until something resolves the factory: that emptiness is
    what tells the bundle there is nothing to close.
    """

    __slots__ = ("factory",)

    def __init__(self) -> None:
        """Start with nothing built."""
        self.factory: TransportFactory | None = None


def combined_transport_factory(built: BuiltTransports) -> TransportFactory:
    """Build one TransportFactory discovering adapters by DSN scheme, on demand.

    Discovery mode on purpose: only the adapter a configured scheme needs is
    imported, when something first asks for it, so an application that only
    speaks ``sync://`` never pays to import a broker library.
    """
    factory = TransportFactory()
    built.factory = factory
    return factory


def combined_transport_factory_with(
    registered: Sequence[TransportFactoryInterface], built: BuiltTransports
) -> TransportFactory:
    """Build one TransportFactory, ``registered`` factories ahead of discovery.

    An application — or another bundle — registering a class under
    :class:`TransportFactoryInterface` gets its factory consulted first, in
    registration order, before a factory discovered by DSN scheme — and
    discovery stays lazy, importing only the adapter a scheme needs. The
    bundle swaps this variant in from :meth:`MessengerBundle.process` only when
    at least one such factory exists, because the engine cannot resolve an empty
    ``Sequence[TransportFactoryInterface]``; with none registered the
    discovery-only :func:`combined_transport_factory` is used instead.
    """
    factory = TransportFactory([*registered, TransportFactory()])
    built.factory = factory
    return factory


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
    config: MessageBusConfig, middleware: ServiceLocator[MiddlewareInterface]
) -> _NamedMiddleware:
    """Resolve the configuration's named middleware once, as a shared singleton."""
    return _NamedMiddleware(await _named_from_config(config, middleware))


async def _named_from_config(
    config: MessageBusConfig, middleware: ServiceLocator[MiddlewareInterface]
) -> dict[str, Callable[[], MiddlewareInterface]]:
    """Build the configured names the container provides, each wrapped to be returned as-is.

    The locator carries every middleware registered under
    :class:`MiddlewareInterface` — those :func:`~xtr_messenger.decorator.as_middleware`
    declared, the ``"logging"`` one, and an entry given arguments under its
    :func:`entry_key`. A configured name the locator does not carry is left to
    the chain's own layered resolution — the process-wide registry, then the
    library's defaults — so a bare name need not be a container service.
    """
    built: dict[str, MiddlewareInterface] = {}
    for name in _middleware_keys(config):
        if name in middleware:
            built[name] = await middleware.get(name)

    def _make_returning(resolved: MiddlewareInterface) -> Callable[[], MiddlewareInterface]:
        return lambda: resolved

    return {name: _make_returning(resolved) for name, resolved in built.items()}


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


def _in_units_of_work(config: MessageBusConfig, scopes: ScopeFactoryInterface) -> MessageBusConfig:
    """Return ``config`` with a unit of work opened around each message, before its middleware."""
    return replace(config, middleware=(UnitOfWorkMiddleware(scopes), *config.middleware))


def message_bus(
    config: MessageBusConfig,
    handlers: HandlersLocator,
    transports: TransportFactory,
    named: _NamedMiddleware,
    scopes: ScopeFactoryInterface,
) -> MessageBusInterface:
    """Build the bus — its named middleware resolved once, shared with the worker factory.

    Every message dispatched through it is a unit of work.
    """
    return MessageBusFactory(
        _in_units_of_work(config, scopes), [transports], handlers, named=named.builders
    ).bus()


async def worker_factory(  # noqa: PLR0913, PLR0917 — one parameter per injected service
    config: MessageBusConfig,
    handlers: HandlersLocator,
    transports: TransportFactory,
    named: _NamedMiddleware,
    resetter: ServicesResetter,
    scopes: ScopeFactoryInterface,
    receivers: ServiceLocator[ReceiverInterface],
    dispatchers: ServiceLocator[EventDispatcherInterface],
) -> WorkerFactory:
    """Build the worker factory — every worker resets services after each message.

    Every message a worker handles is a unit of work. Workers announce
    themselves and their messages through the event dispatcher when one is
    registered — the ``dispatchers`` locator carries it keyed ``None`` — and
    stay silent otherwise, where the locator is empty. Every tagged receiver
    the bundle aliased under :class:`ReceiverInterface` is built here,
    consumable under the alias that keys it.
    """
    built_receivers: dict[str, ReceiverInterface] = {}
    async for alias, receiver in receivers:
        built_receivers[str(alias)] = receiver
    event_dispatcher = await dispatchers.get(None) if None in dispatchers else None
    return WorkerFactory(
        _in_units_of_work(config, scopes),
        [transports],
        handlers,
        named=named.builders,
        resetter=cast("AsyncResetter", resetter),
        event_dispatcher=event_dispatcher,
        receivers=built_receivers,
    )
