"""The xtr-messenger bundle: wire a bus and worker factory into a kernel.

Install with the ``di`` extra and list :class:`MessengerBundle` in
``<package>/bundles.py``. The logging and console bundles are pulled in as
required bundles when installed, but the messenger keeps working without
either.
"""

from __future__ import annotations

from xtr_messenger.message_bus_config import MessageBusConfig
from xtr_messenger.transport.transport_config import TransportConfig

from .messenger_bundle import MessengerBundle

__all__ = ["MessageBusConfig", "MessengerBundle", "TransportConfig"]
