"""A cursor over the middleware remaining for one dispatch."""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from .middleware_interface import MiddlewareInterface

__all__ = ["StackInterface"]


@runtime_checkable
class StackInterface(Protocol):
    """A cursor over the middleware remaining for one dispatch."""

    def next(self) -> MiddlewareInterface:
        """Return the next middleware, or an identity tail once exhausted."""
        ...
