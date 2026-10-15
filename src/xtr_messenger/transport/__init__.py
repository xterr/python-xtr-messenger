"""Transports shipped with the library.

These two need no third-party dependency. Those that do live under
:mod:`xtr_messenger.bridge`: the taskiq bridge, on taskiq, and RabbitMQ on
top of it, with the ``amqp`` extra.
"""

from __future__ import annotations

from .in_memory.in_memory_transport import InMemoryTransport
from .sync.sync_transport import SyncTransport

__all__ = ["InMemoryTransport", "SyncTransport"]
