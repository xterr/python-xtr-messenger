"""The registry handler declarations accumulate in."""

from __future__ import annotations

from .handlers_locator import HandlersLocator

__all__ = ["default_registry"]

_DEFAULT_REGISTRY = HandlersLocator()


def default_registry() -> HandlersLocator:
    """Return the registry ``@as_message_handler`` fills when given no other.

    Import-time registration needs somewhere to accumulate; this is it. Pass
    an explicit registry when you want isolation, as tests generally should.
    """
    return _DEFAULT_REGISTRY
