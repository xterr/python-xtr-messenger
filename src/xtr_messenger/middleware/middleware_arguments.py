"""A chain entry giving middleware arguments by name: ``{name: {argument: value}}``."""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, TypeAlias, cast

from xtr_messenger.exception import InvalidMiddlewareArgumentsError

if TYPE_CHECKING:
    from .middleware_interface import MiddlewareInterface

__all__ = ["MiddlewareArguments", "MiddlewareEntry", "entry_arguments", "entry_key"]

MiddlewareArguments: TypeAlias = "Mapping[str, object]"
"""A middleware's arguments, by the names of the parameters they fill."""

MiddlewareEntry: TypeAlias = "str | MiddlewareInterface | Mapping[str, MiddlewareArguments]"
"""One entry of a chain: a name, middleware already built, or ``{name: arguments}``."""


def entry_arguments(entry: Mapping[str, MiddlewareArguments]) -> tuple[str, MiddlewareArguments]:
    """Return the name and arguments of a ``{name: arguments}`` entry.

    Raises:
        InvalidMiddlewareArgumentsError: If the entry names no middleware or
            several, or its arguments are not a mapping of parameter names.
    """
    if len(entry) != 1:
        names = ", ".join(repr(name) for name in entry) or "none"
        raise InvalidMiddlewareArgumentsError(
            names, "an entry with arguments is a single {name: {argument: value}}"
        )
    [(name, arguments)] = entry.items()
    if not isinstance(arguments, Mapping) or not all(  # pyright: ignore[reportUnnecessaryIsInstance] -- configs are written by hand; the annotation is not enforced
        isinstance(key, str) for key in cast("Mapping[object, object]", arguments)
    ):
        raise InvalidMiddlewareArgumentsError(
            name, f"arguments are a mapping of parameter names, not {arguments!r}"
        )
    return name, arguments


def entry_key(name: str, ordinal: int) -> str:
    """Return what a container registers the middleware of an entry with arguments under.

    The entry's arguments make it a middleware of its own, beside the one
    ``name`` stands for bare. ``ordinal`` is its place among the chain's
    entries with arguments — counted among those alone, so middleware put
    ahead of the chain already built does not move it — which tells it apart
    from another entry for the same name.
    """
    return f"{name}#{ordinal}"
