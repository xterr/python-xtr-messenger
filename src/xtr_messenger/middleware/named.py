"""The middleware chain a configuration describes, and what its names mean."""

from __future__ import annotations

import inspect
from collections.abc import Mapping
from typing import TYPE_CHECKING, TypeAlias, cast, final

from typing_extensions import override

from xtr_messenger.exception import InvalidMiddlewareArgumentsError, UnknownMiddlewareError

from .dispatch_after_current_bus_middleware import DispatchAfterCurrentBusMiddleware
from .logging_middleware import LoggingMiddleware
from .middleware_arguments import entry_arguments, entry_key
from .middleware_registry import default_middleware_registry

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator, Sequence

    from xtr_logging_contracts import LoggerInterface

    from xtr_messenger.message_bus_config import MessageBusConfig

    from .middleware_arguments import MiddlewareEntry
    from .middleware_interface import MiddlewareInterface
    from .middleware_registry import MiddlewareRegistry

__all__ = ["MiddlewareBuilder", "chain", "named_middleware", "resolve"]

MiddlewareBuilder: TypeAlias = "Callable[..., MiddlewareInterface]"
"""Builds one middleware, taking whatever it needs from where it was declared.

Called with no argument for a bare name; for an entry carrying arguments,
with them as keywords.
"""


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

    With ``default_middleware`` on, a
    :class:`~xtr_messenger.middleware.DispatchAfterCurrentBusMiddleware` comes
    first, so a message held back until the current one was handled waits for
    everything the configured middleware do around it. ``defaults`` is only
    called when ``default_middleware`` is on, so what it would open — a
    transport's connection — is never opened for a chain that leaves it out.

    Raises:
        UnknownMiddlewareError: If ``config`` names middleware ``named``
            has nothing registered for.
    """
    configured = list(resolve(config.middleware, named))
    if not config.default_middleware:
        return configured
    return [DispatchAfterCurrentBusMiddleware(), *configured, *defaults()]


def resolve(
    configured: Sequence[MiddlewareEntry],
    named: Mapping[str, MiddlewareBuilder],
) -> tuple[MiddlewareInterface, ...]:
    """Return what ``configured`` asks for, built in the order it names it.

    An entry is a name to look up in ``named``, middleware already built, or
    ``{name: arguments}``: the middleware ``name`` stands for, given
    ``arguments`` — built by ``named`` under :func:`entry_key` when a
    container prepared it, otherwise by the builder of ``name`` with them as
    keywords.

    Raises:
        UnknownMiddlewareError: If a name is not registered.
        InvalidMiddlewareArgumentsError: If an entry's arguments are not ones
            its middleware takes.
    """
    built: list[MiddlewareInterface] = []
    ordinal = 0
    for entry in configured:
        if isinstance(entry, str):
            built.append(_builder(named, entry)())
        elif isinstance(entry, Mapping):
            name, arguments = entry_arguments(entry)
            prepared = named.get(entry_key(name, ordinal))
            ordinal += 1
            if prepared is not None:
                built.append(prepared())
                continue
            builder = _builder(named, name)
            try:
                _ = inspect.signature(builder).bind(**arguments)
            except TypeError as error:
                raise InvalidMiddlewareArgumentsError(name, str(error)) from error
            built.append(builder(**arguments))
        else:
            built.append(entry)
    return tuple(built)


def _builder(named: Mapping[str, MiddlewareBuilder], name: str) -> MiddlewareBuilder:
    builder = named.get(name)
    if builder is None:
        raise UnknownMiddlewareError(name, tuple(named))
    return builder
