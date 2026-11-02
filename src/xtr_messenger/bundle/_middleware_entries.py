"""Registering a middleware of its own for each chain entry given arguments."""

from __future__ import annotations

from collections.abc import Mapping
from typing import cast

from xtr_dependency_injection import ContainerBuilder

from xtr_messenger.exception import InvalidMiddlewareArgumentsError, UnknownMiddlewareError
from xtr_messenger.message_bus_config import MessageBusConfig
from xtr_messenger.middleware.middleware_arguments import entry_arguments, entry_key
from xtr_messenger.middleware.middleware_interface import MiddlewareInterface

__all__ = ["register_middleware_with_arguments"]


def register_middleware_with_arguments(builder: ContainerBuilder) -> None:
    """Register a middleware of its own for every chain entry that gives one arguments.

    It is built like the middleware the name stands for — the arguments its
    definition already has kept, the entry's set over them on the parameters
    they name — and registered under
    :func:`~xtr_messenger.middleware.middleware_arguments.entry_key` — so the
    entry and the bare name, or two entries with different arguments, are
    separate instances. An argument naming no parameter fails the build, as
    any definition argument does.

    Raises:
        UnknownMiddlewareError: If an entry names middleware nothing is
            registered for.
        InvalidMiddlewareArgumentsError: If the middleware is built by a
            factory.
    """
    config = builder.get_extension_config(MessageBusConfig)
    entries = [entry for entry in config.middleware if isinstance(entry, Mapping)]
    for ordinal, entry in enumerate(entries):
        name, arguments = entry_arguments(entry)
        if not builder.has(MiddlewareInterface, name):
            known = tuple(
                str(alias[1])
                for alias in builder.get_aliases()
                if alias[0] is MiddlewareInterface and isinstance(alias[1], str)
            )
            raise UnknownMiddlewareError(name, known)
        named = builder.find_definition(MiddlewareInterface, name)
        if named.kind != "class" or not isinstance(named.provider, type):
            raise InvalidMiddlewareArgumentsError(
                name, "it is built by a factory, which a configuration cannot give arguments"
            )
        middleware_cls = cast("type[MiddlewareInterface]", named.provider)
        key = entry_key(name, ordinal)
        _ = builder.register(middleware_cls, qualifier=key).set_arguments(
            {**named.get_arguments(), **arguments}
        )
        builder.set_alias(
            MiddlewareInterface, middleware_cls, alias_qualifier=key, target_qualifier=key
        )
