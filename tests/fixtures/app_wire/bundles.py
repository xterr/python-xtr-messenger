"""The application's root bundles: only MessengerBundle — the serializer comes along."""

from __future__ import annotations

from xtr_messenger.bundle import MessengerBundle

BUNDLES = {MessengerBundle: {"all": True}}
