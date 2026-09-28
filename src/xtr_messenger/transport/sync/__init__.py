"""The ``sync://`` transport, which handles in the calling process."""

from __future__ import annotations

from .sync_transport import SyncTransport
from .sync_transport_factory import SYNC_SCHEME, SyncTransportFactory

__all__ = ["SYNC_SCHEME", "SyncTransport", "SyncTransportFactory"]
