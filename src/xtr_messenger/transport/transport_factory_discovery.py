"""Finding the transport factory that serves a DSN.

Factories are discovered, not listed. Any installed distribution can offer
one by advertising it under the ``xtr_messenger.transport_factories`` entry
point group, so adding a transport never means editing this package::

    [project.entry-points."xtr_messenger.transport_factories"]
    kafka = "my_package.kafka:KafkaTransportFactory"

**The entry point name is the DSN scheme it serves.** That is what makes
resolution lazy: a ``sync://`` transport is found by name and only its module
is imported, so an application that never speaks AMQP never pays to import a
broker library. Declare one entry point per scheme a factory serves.

A scheme whose dependency is absent simply has no usable factory, which is
how an optional extra stays optional: the entry point is always advertised,
and only becomes usable once its dependency is installed.

**Discovered factories are built with no settings**, which is why
configuration does not reach them here. It reaches them per call instead:
every factory method receives the
:class:`~xtr_messenger.transport.transport_config.TransportConfig` of each
transport it is building, and reads
:attr:`~xtr_messenger.transport.transport_config.TransportConfig.settings` off
it — the DSN query string merged with ``options``. Settings belong to a
transport, not to the factory, so one discovered ``amqp://`` factory serves
two transports pointing at different queues with different retry policies.

One collaborator is handed over: the envelope serializer the bus writes
messages with. A factory whose constructor takes a ``serializer`` parameter
is built with the one discovery was given; one that requires it, discovered
with none, is refused with
:class:`~xtr_messenger.exception.MissingSerializerError`. Any other
collaborator — a private handlers locator, anything that is an object rather
than a string — is a constructor argument, so supplying one means passing the
factory yourself::

    from xtr_amqp_messenger import AmqpTransportFactory
    from xtr_amqp_messenger.transport.amqp_options import AmqpOptions

    MessageBusFactory(config, [AmqpTransportFactory(mine, options=AmqpOptions(...))])

Discovery is a default, never a mandate.
"""

from __future__ import annotations

import inspect
from importlib.metadata import entry_points
from typing import TYPE_CHECKING, cast

from xtr_messenger.exception import MissingSerializerError

from .transport_factory_interface import TransportFactoryInterface

if TYPE_CHECKING:
    from collections.abc import Callable
    from importlib.metadata import EntryPoint

    from xtr_messenger.dsn import Dsn
    from xtr_messenger.transport.serialization import SerializerInterface

__all__ = [
    "ENTRY_POINT_GROUP",
    "advertised_schemes",
    "default_factories",
    "factory_for",
]

#: The entry point group third-party distributions advertise under. Plural
#: because it names a group holding many factories, following the convention
#: of ``sqlalchemy.dialects`` and ``fsspec.specs``.
#:
#: Deliberately not tied to this module's name, which has changed twice while
#: this has not. Nothing external depends on it yet — the package is
#: unreleased — but it is the one string a third-party adapter must hard-code,
#: so it is worth settling before the first release rather than after.
ENTRY_POINT_GROUP = "xtr_messenger.transport_factories"


def advertised_schemes() -> tuple[str, ...]:
    """Return every DSN scheme an installed distribution advertises.

    Advertised is not the same as usable: a scheme whose dependency is
    missing appears here but yields no factory.
    """
    return tuple(sorted(entry.name for entry in entry_points(group=ENTRY_POINT_GROUP)))


def factory_for(
    dsn: Dsn, serializer: SerializerInterface | None = None
) -> TransportFactoryInterface | None:
    """Return the factory serving ``dsn``, importing only that one.

    Returns ``None`` when no distribution advertises the scheme, or when the
    one that does cannot be loaded because its dependency is absent.

    Raises:
        MissingSerializerError: If the factory serving ``dsn`` needs a
            ``serializer`` and none is given.
    """
    for entry in entry_points(group=ENTRY_POINT_GROUP, name=dsn.scheme):
        factory = _build(entry, serializer)
        if factory is not None and factory.supports(dsn):
            return factory
    return None


def default_factories(
    serializer: SerializerInterface | None = None,
) -> tuple[TransportFactoryInterface, ...]:
    """Return every usable factory, importing all of them.

    Resolution does not need this — it loads one factory per scheme on
    demand. Use it to compose an explicit list alongside your own::

        MessageBusFactory(config, [MyFactory(), *default_factories(serializer)])

    Raises:
        MissingSerializerError: If an installed factory needs a
            ``serializer`` and none is given.
    """
    found: list[TransportFactoryInterface] = []
    seen: set[str] = set()
    for entry in sorted(entry_points(group=ENTRY_POINT_GROUP), key=lambda e: e.name):
        if entry.value in seen:
            continue
        factory = _build(entry, serializer)
        if factory is not None:
            seen.add(entry.value)
            found.append(factory)
    return tuple(found)


def _build(
    entry: EntryPoint, serializer: SerializerInterface | None
) -> TransportFactoryInterface | None:
    """Instantiate one advertised factory, or return ``None`` if unusable.

    Built with no settings on purpose. A factory serves every transport on
    its scheme, and those transports are configured differently from one
    another, so there is nothing here that could be passed — each factory
    method reads the settings of the transports it is handed instead. The
    envelope serializer is the exception: it is the same for every transport,
    so a factory asking for one by a ``serializer`` parameter is given it.

    The entry point is loaded through a reference typed to return ``object``
    so nothing untyped escapes, and the result is checked against the
    protocol — a distribution advertising something that is not a transport
    factory is skipped rather than breaking every other transport.

    Raises:
        MissingSerializerError: If the factory requires a ``serializer`` and
            ``serializer`` is ``None``.
    """
    load: Callable[[], object] = entry.load
    try:
        advertised = load()
    except ImportError:
        return None
    if not callable(advertised):
        return None
    build: Callable[..., object] = advertised
    wanted = inspect.signature(build).parameters.get("serializer")
    if wanted is None:
        made = build()
    elif serializer is not None:
        made = build(serializer=serializer)
    elif cast("object", wanted.default) is inspect.Parameter.empty:
        reason = "its factory requires one; build TransportFactory with serializer=..."
        raise MissingSerializerError(entry.name, reason)
    else:
        made = build()
    return made if isinstance(made, TransportFactoryInterface) else None
