"""The middleware chain a configuration describes, and what its names mean."""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, TypeAlias, cast, final

from typing_extensions import override

from xtr_messenger.exception import UnknownMiddlewareError

from .logging_middleware import LoggingMiddleware
from .middleware_registry import default_middleware_registry

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator, Sequence

    from xtr_logging_contracts import LoggerInterface

    from xtr_messenger.message_bus_config import MessageBusConfig

    from .middleware_interface import MiddlewareInterface
    from .middleware_registry import MiddlewareRegistry

__all__ = ["MiddlewareBuilder", "chain", "named_middleware", "resolve"]

MiddlewareBuilder: TypeAlias = "Callable[[], MiddlewareInterface]"
"""Builds one middleware, taking whatever it needs from where it was declared."""


def named_middleware(
    logger: LoggerInterface | None = None,
    named: Mapping[str, MiddlewareBuilder] | None = None,
    registry: MiddlewareRegistry | None = None,
) -> Mapping[str, MiddlewareBuilder]:
    """Return what each name means: ``named``, then ``registry``, then ``"logging"``.

    ``registry`` — the process-wide one by default — holds the classes
    :func:`~xtr_messenger.decorator.as_middleware` declared, each built with
    no argument where a chain names it; it is read as the chain is built, so
    a class declared later still counts. A name in ``named`` wins over one
    declared, which wins over the library's own.
    """
    declared = registry if registry is not None else default_middleware_registry()
    library: dict[str, MiddlewareBuilder] = {"logging": lambda: LoggingMiddleware(logger)}
    return _Names((dict(named or {}), cast("Mapping[str, MiddlewareBuilder]", declared), library))


@final
class _Names(Mapping[str, "MiddlewareBuilder"]):
    """Names read through layers, the first that has one winning; listed library first."""

    __slots__ = ("_layers",)

    def __init__(self, layers: tuple[Mapping[str, MiddlewareBuilder], ...]) -> None:
        self._layers = layers

    @override
    def __getitem__(self, name: str, /) -> MiddlewareBuilder:
        for layer in self._layers:
            if name in layer:
                return layer[name]
        raise KeyError(name)

    @override
    def __iter__(self) -> Iterator[str]:
        return iter(dict.fromkeys(name for layer in reversed(self._layers) for name in layer))

    @override
    def __len__(self) -> int:
        return sum(1 for _ in self)


def chain(
    config: MessageBusConfig,
    named: Mapping[str, MiddlewareBuilder],
    defaults: Callable[[], Sequence[MiddlewareInterface]],
) -> list[MiddlewareInterface]:
    """Return the chain ``config`` describes: what it names, then ``defaults``.

    ``defaults`` is only called when ``default_middleware`` is on, so what it
    would open — a transport's connection — is never opened for a chain
    that leaves it out.

    Raises:
        UnknownMiddlewareError: If ``config`` names middleware ``named``
            has nothing registered for.
    """
    configured = list(resolve(config.middleware, named))
    return [*configured, *defaults()] if config.default_middleware else configured


def resolve(
    configured: Sequence[str | MiddlewareInterface],
    named: Mapping[str, MiddlewareBuilder],
) -> tuple[MiddlewareInterface, ...]:
    """Return what ``configured`` asks for, built in the order it names it.

    An entry is a name to look up in ``named``, or middleware already built.

    Raises:
        UnknownMiddlewareError: If a name is not registered.
    """
    built: list[MiddlewareInterface] = []
    for entry in configured:
        if isinstance(entry, str):
            builder = named.get(entry)
            if builder is None:
                raise UnknownMiddlewareError(entry, tuple(named))
            built.append(builder())
        else:
            built.append(entry)
    return tuple(built)
