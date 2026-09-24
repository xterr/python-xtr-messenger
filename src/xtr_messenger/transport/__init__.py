"""Transports shipped with the library.

These two need no third-party dependency. The taskiq adapter lives in
:mod:`xtr_messenger.bridge.amqp` and is installed with the ``taskiq`` extra.
"""

from .in_memory.in_memory_transport import InMemoryTransport
from .sync.sync_transport import SyncTransport

__all__ = ["InMemoryTransport", "SyncTransport"]
