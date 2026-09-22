"""The registry handler declarations accumulate in."""

from __future__ import annotations

from typing import TYPE_CHECKING

from .handlers_locator import HandlersLocator

if TYPE_CHECKING:
    from .handlers_locator_interface import HandlersLocatorInterface

__all__ = ["default_registry"]

_DEFAULT_REGISTRY = HandlersLocator()


def default_registry() -> HandlersLocatorInterface:
    """Return the registry ``@as_message_handler`` fills when given no other.

    Import-time registration needs somewhere to accumulate; this is it. Pass
    an explicit registry when you want isolation, as tests generally should.
    """
    return _DEFAULT_REGISTRY
