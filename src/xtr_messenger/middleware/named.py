"""The middleware chain a configuration describes, and what its names mean."""

from __future__ import annotations

from typing import TYPE_CHECKING, TypeAlias

from xtr_messenger.exception import UnknownMiddlewareError

from .logging_middleware import LoggingMiddleware

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping, Sequence

    from xtr_logging import LoggerInterface

    from xtr_messenger.message_bus_config import MessageBusConfig

    from .middleware_interface import MiddlewareInterface

__all__ = ["MiddlewareBuilder", "chain", "named_middleware", "resolve"]

MiddlewareBuilder: TypeAlias = "Callable[[], MiddlewareInterface]"
"""Builds one middleware, taking whatever it needs from where it was declared."""


def named_middleware(
    logger: LoggerInterface | None = None,
    named: Mapping[str, MiddlewareBuilder] | None = None,
) -> dict[str, MiddlewareBuilder]:
    """Return what each name means: ``"logging"``, through ``logger``, then ``named``.

    A name in ``named`` replaces the library's own.
    """
    return {"logging": lambda: LoggingMiddleware(logger), **(named or {})}


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
