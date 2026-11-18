"""The messenger panel's services, registered while the http kernel's profiler is on.

Imported only then, since it needs xtr-http-kernel. Every factory here is read
by the container, so the types its parameters name are imported at runtime.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Annotated, Final

from xtr_dependency_injection import AutowireDecorated
from xtr_http_kernel.data_collector import DATA_COLLECTOR_TAG

from xtr_messenger.data_collector.messenger_data_collector import MessengerDataCollector
from xtr_messenger.data_collector.traceable_message_bus import TraceableMessageBus
from xtr_messenger.message_bus_interface import MessageBusInterface

if TYPE_CHECKING:
    from xtr_dependency_injection import ServiceConfigurator

__all__ = ["register_profiler", "traceable_message_bus"]

_NAME: Final = "messenger"
_TEMPLATE: Final = "@xtr_messenger/collector/messenger.html.j2"
_PRIORITY: Final = 240


def register_profiler(services: ServiceConfigurator) -> None:
    """Register the collector, and trace the container's bus into it."""
    _ = services.set(MessengerDataCollector).add_tag(
        DATA_COLLECTOR_TAG, name=_NAME, template=_TEMPLATE, priority=_PRIORITY
    )
    _ = services.set(traceable_message_bus).set_decorated_service(MessageBusInterface)


def traceable_message_bus(
    bus: Annotated[MessageBusInterface, AutowireDecorated()],
    collector: MessengerDataCollector,
) -> TraceableMessageBus:
    """Trace ``bus`` into the trace ``collector`` reads."""
    return TraceableMessageBus(bus, collector.trace)
