"""Middleware classes declared by name, waiting for a factory or a bundle."""

from __future__ import annotations

from typing import TYPE_CHECKING, cast, final

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator, Mapping

    from .middleware_interface import MiddlewareInterface

__all__ = [
    "MIDDLEWARE_ATTRIBUTE",
    "MiddlewareRegistry",
    "default_middleware_registry",
    "middleware_declared_on",
]

MIDDLEWARE_ATTRIBUTE = "__xtr_messenger_middleware_names__"
"""Where :func:`~xtr_messenger.decorator.as_middleware` records the names on a class."""


def middleware_declared_on(obj: object) -> Iterable[str]:
    """Yield every name :func:`~xtr_messenger.decorator.as_middleware` put on ``obj``.

    A reader for
    :meth:`~xtr_dependency_injection.builder.ContainerBuilder.register_attribute_for_autoconfiguration`:
    the messenger bundle uses it so a middleware class becomes a container
    service under each of the names its configuration may refer to.
    """
    if not isinstance(obj, type):
        return ()
    declarations: object = getattr(obj, MIDDLEWARE_ATTRIBUTE, ())
    if not isinstance(declarations, tuple):
        return ()
    typed = cast("tuple[object, ...]", declarations)
    return tuple(entry for entry in typed if isinstance(entry, str))


@final
class MiddlewareRegistry:
    """Holds middleware classes by the names a configuration refers to them by.

    :func:`~xtr_messenger.decorator.as_middleware` writes to the process-wide
    one by default, which is what lets a module declare a middleware without
    importing a factory or a bundle. Pass a registry of your own to keep two
    applications — or two tests — apart.
    """

    __slots__ = ("_declared",)

    def __init__(self) -> None:
        """Start empty."""
        self._declared: dict[str, type[MiddlewareInterface]] = {}

    def register(self, name: str, cls: type[MiddlewareInterface], /) -> None:
        """Bind ``name`` to ``cls`` — the last registration for a name wins."""
        self._declared[name] = cls

    def get(self, name: str, /) -> type[MiddlewareInterface] | None:
        """Return the class bound to ``name``, or ``None`` when nothing is."""
        return self._declared.get(name)

    def items(self) -> Iterable[tuple[str, type[MiddlewareInterface]]]:
        """Yield every ``(name, class)`` pair in declaration order."""
        return tuple(self._declared.items())

    def __contains__(self, name: object) -> bool:
        """Return whether ``name`` is bound to a class here."""
        return name in self._declared

    def __iter__(self) -> Iterator[str]:
        """Yield each declared name."""
        return iter(self._declared)

    def __len__(self) -> int:
        """Return how many names are bound."""
        return len(self._declared)

    def as_mapping(self) -> Mapping[str, type[MiddlewareInterface]]:
        """Return every registration as a plain mapping — a snapshot copy."""
        return dict(self._declared)

    def clear(self) -> None:
        """Forget every registration."""
        self._declared.clear()


_DEFAULT = MiddlewareRegistry()


def default_middleware_registry() -> MiddlewareRegistry:
    """Return the process-wide registry :func:`as_middleware` declares into."""
    return _DEFAULT
