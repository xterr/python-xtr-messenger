"""The sending half of a transport, and the table that routes to it."""

from __future__ import annotations

from .sender_interface import SenderInterface
from .senders_locator import WILDCARD, SendersLocator
from .senders_locator_interface import SendersLocatorInterface

__all__ = ["WILDCARD", "SenderInterface", "SendersLocator", "SendersLocatorInterface"]
