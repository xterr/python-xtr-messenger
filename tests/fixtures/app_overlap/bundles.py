"""The application's root bundles: the messenger, and logging beside it."""

from __future__ import annotations

from xtr_logging.bundle import LoggingBundle

from xtr_messenger.bundle import MessengerBundle

BUNDLES = {MessengerBundle: {"all": True}, LoggingBundle: {"all": True}}
