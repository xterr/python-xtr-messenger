"""Naming a middleware where it is written, for a factory or a bundle to attach."""

from __future__ import annotations

from typing import TYPE_CHECKING, TypeVar, cast

from xtr_messenger.middleware.middleware_registry import (
    MIDDLEWARE_ATTRIBUTE,
    default_middleware_registry,
)

if TYPE_CHECKING:
    from collections.abc import Callable

    from xtr_messenger.middleware.middleware_interface import MiddlewareInterface
    from xtr_messenger.middleware.middleware_registry import MiddlewareRegistry

__all__ = ["as_middleware"]

M = TypeVar("M", bound="type[MiddlewareInterface]")


def as_middleware(
    name: str,
    /,
    *,
    registry: MiddlewareRegistry | None = None,
) -> Callable[[M], M]:
    """Declare the decorated middleware class under ``name``.

    A configuration's ``middleware`` list refers to middleware by name; this
    binds a class to that name in a registry, and repeats the name on the
    class as ``__xtr_messenger_middleware_names__``, so both a factory
    (container-less) and the bundle (via
    :func:`~xtr_messenger.middleware.middleware_registry.middleware_declared_on`)
    can attach it.

    Repeat the decorator to give the same class more than one name — every
    name reaches the same service under a container.

    Args:
        name: The name a configuration refers to this middleware by.
        registry: Declare here instead of the process-wide registry.
    """
    target_registry = registry if registry is not None else default_middleware_registry()

    def declare(target: M) -> M:
        middleware_cls = cast("type[MiddlewareInterface]", target)
        target_registry.register(name, middleware_cls)
        existing: object = getattr(target, MIDDLEWARE_ATTRIBUTE, ())
        previous = cast("tuple[str, ...]", existing) if isinstance(existing, tuple) else ()
        setattr(target, MIDDLEWARE_ATTRIBUTE, (*previous, name))
        return target

    return declare
