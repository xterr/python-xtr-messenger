"""The application's root bundles: messaging and event dispatching."""

from __future__ import annotations

from xtr_event_dispatcher.bundle import EventDispatcherBundle

from xtr_messenger.bundle import MessengerBundle

BUNDLES = {MessengerBundle: {"all": True}, EventDispatcherBundle: {"all": True}}
