"""The application's root bundles: the console and the messenger."""

from __future__ import annotations

from xtr_console.bundle import ConsoleBundle

from xtr_messenger.bundle import MessengerBundle

BUNDLES = {ConsoleBundle: {"all": True}, MessengerBundle: {"all": True}}
