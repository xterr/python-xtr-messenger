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

**Discovered factories are built with no arguments**, which is why
configuration does not reach them here. It reaches them per call instead:
every factory method receives the
:class:`~xtr_messenger.transport.transport_config.TransportConfig` of each
transport it is building, and reads
:attr:`~xtr_messenger.transport.transport_config.TransportConfig.settings` off
it — the DSN query string merged with ``options``. Settings belong to a
transport, not to the factory, so one discovered ``amqp://`` factory serves
two transports pointing at different queues with different retry policies.

What cannot arrive that way is a collaborator: a serializer, a private
handlers locator, anything that is an object rather than a string. Those are
constructor arguments, so supplying one means passing the factory yourself::

    MessageBusFactory(config, [AmqpTransportFactory(serializer=mine)])

Discovery is a default, never a mandate.
"""

from __future__ import annotations

from importlib.metadata import entry_points
from typing import TYPE_CHECKING

from .transport_factory_interface import TransportFactoryInterface

if TYPE_CHECKING:
    from collections.abc import Callable
    from importlib.metadata import EntryPoint

    from xtr_messenger.dsn import Dsn

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


def factory_for(dsn: Dsn) -> TransportFactoryInterface | None:
    """Return the factory serving ``dsn``, importing only that one.

    Returns ``None`` when no distribution advertises the scheme, or when the
    one that does cannot be loaded because its dependency is absent.
    """
    for entry in entry_points(group=ENTRY_POINT_GROUP, name=dsn.scheme):
        factory = _build(entry)
        if factory is not None and factory.supports(dsn):
            return factory
    return None


def default_factories() -> tuple[TransportFactoryInterface, ...]:
    """Return every usable factory, importing all of them.

    Resolution does not need this — it loads one factory per scheme on
    demand. Use it to compose an explicit list alongside your own::

        MessageBusFactory(config, [MyFactory(), *default_factories()])
    """
    found: list[TransportFactoryInterface] = []
    seen: set[str] = set()
    for entry in sorted(entry_points(group=ENTRY_POINT_GROUP), key=lambda e: e.name):
        if entry.value in seen:
            continue
        factory = _build(entry)
        if factory is not None:
            seen.add(entry.value)
            found.append(factory)
    return tuple(found)


def _build(entry: EntryPoint) -> TransportFactoryInterface | None:
    """Instantiate one advertised factory, or return ``None`` if unusable.

    Built with no arguments on purpose. A factory serves every transport on
    its scheme, and those transports are configured differently from one
    another, so there is nothing here that could be passed — each factory
    method reads the settings of the transports it is handed instead.

    The entry point is loaded through a reference typed to return ``object``
    so nothing untyped escapes, and the result is checked against the
    protocol — a distribution advertising something that is not a transport
    factory is skipped rather than breaking every other transport.
    """
    load: Callable[[], object] = entry.load
    try:
        advertised = load()
    except ImportError:
        return None
    if not callable(advertised):
        return None
    build: Callable[[], object] = advertised
    made = build()
    return made if isinstance(made, TransportFactoryInterface) else None
