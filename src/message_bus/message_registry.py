"""What the application has declared about its message classes.

Two things are recorded per message type, both written by
:func:`~message_bus.decorator.as_message.as_message` and read by the
serializer and the routing table:

``name``
    The identity the message travels under, and the contract between
    producer and consumer — both import the message class, so neither needs
    to import the other's code, and a producer's import graph never drags in
    handlers or a database driver. Defaults to ``module:QualName``, which is
    convenient but changes if the class moves, so anything that outlives a
    deploy should pin an explicit, versioned name.

``transport``
    Where the message goes when no routing table entry matches. A routing
    table always wins; this is the fallback, so a message can carry a sane
    default without every application repeating it.

The store is process-wide because declaration happens at import time, as a
side effect of defining the class. Nothing here is per-bus: two buses in one
process carry the same message under the same name.
"""

from __future__ import annotations

import importlib

from .exception import MessageBusError, UnknownMessageNameError

__all__ = ["declared_names", "name_of", "register_message", "transports_of", "type_for_name"]

_NAME_BY_TYPE: dict[type, str] = {}
_TYPE_BY_NAME: dict[str, type] = {}
_TRANSPORTS_BY_TYPE: dict[type, tuple[str, ...]] = {}


def register_message(
    message_type: type,
    name: str | None = None,
    transports: tuple[str, ...] = (),
) -> None:
    """Record ``message_type``'s identity and default transport.

    The storage side of :func:`~message_bus.decorator.as_message.as_message`,
    kept separate so the registry can be written to without decorator syntax
    — for a class built dynamically, or one whose name is only known at
    import time.

    Raises:
        MessageBusError: If ``name`` is already claimed by a different class.
    """
    if name is not None:
        _claim(name, message_type)
    elif message_type not in _NAME_BY_TYPE:
        _claim(_default_name(message_type), message_type)
    if transports:
        _TRANSPORTS_BY_TYPE[message_type] = transports


def declared_names() -> tuple[str, ...]:
    """Return the wire name of every declared message.

    What a consumer that looks messages up by name — a taskiq worker
    registers a task per name — can expect to receive.
    """
    return tuple(_NAME_BY_TYPE.values())


def name_of(message_type: type) -> str:
    """Return the wire name of ``message_type``.

    Falls back to ``module:QualName`` when no explicit name was pinned.
    """
    pinned = _NAME_BY_TYPE.get(message_type)
    if pinned is not None:
        return pinned
    return _default_name(message_type)


def _default_name(message_type: type) -> str:
    return f"{message_type.__module__}:{message_type.__qualname__}"


def transports_of(message_type: type) -> tuple[str, ...]:
    """Return the transports ``message_type`` declares, walking its bases.

    Empty when neither the class nor any of its bases declared one.
    """
    for base in message_type.__mro__:
        declared = _TRANSPORTS_BY_TYPE.get(base)
        if declared:
            return declared
    return ()


def type_for_name(name: str) -> type:
    """Resolve a wire name back to its message class.

    Looks in the registry first, then falls back to importing a derived
    ``module:QualName`` name.

    Raises:
        UnknownMessageNameError: If the name resolves to nothing importable.
    """
    registered = _TYPE_BY_NAME.get(name)
    if registered is not None:
        return registered
    if ":" not in name:
        raise UnknownMessageNameError(name)
    module_name, _, qualname = name.partition(":")
    try:
        module = importlib.import_module(module_name)
    except ImportError as exc:
        raise UnknownMessageNameError(name) from exc
    resolved: object = module
    for part in qualname.split("."):
        resolved = getattr(resolved, part, None)
        if resolved is None:
            raise UnknownMessageNameError(name)
    if not isinstance(resolved, type):
        raise UnknownMessageNameError(name)
    return resolved


def _claim(name: str, message_type: type) -> None:
    claimed = _TYPE_BY_NAME.get(name)
    if claimed is not None and claimed is not message_type:
        raise MessageBusError(
            f"message name {name!r} is already used by {claimed.__qualname__}",
        )
    _NAME_BY_TYPE[message_type] = name
    _TYPE_BY_NAME[name] = message_type
