"""The stamp classes the application has declared sendable.

A stamp crosses the wire under its class name, and a serializer restores
only the classes it knows — decoding names a class this process must build,
so an unknown name is dropped rather than resolved. Declaring a stamp with
:func:`~xtr_messenger.decorator.as_stamp.as_stamp` adds it to what every
serializer built without an explicit list restores, which is how a library
shipping its own stamp gets it through a real transport without every
application passing it along.

The store is process-wide because declaration happens at import time, as a
side effect of defining the class.
"""

from __future__ import annotations

from .exception import MessageBusError
from .stamp import DEFAULT_STAMP_TYPES, StampInterface

__all__ = ["declared_stamps", "register_stamp", "stamp_type_for"]

_DEFAULT_BY_NAME: dict[str, type[StampInterface]] = {t.__name__: t for t in DEFAULT_STAMP_TYPES}
_DECLARED_BY_NAME: dict[str, type[StampInterface]] = {}


def register_stamp(stamp_type: type[StampInterface]) -> None:
    """Record ``stamp_type`` as one serializers restore.

    Raises:
        MessageBusError: If another class already travels under the same
            class name — the header that carries a stamp names only that.
    """
    name = stamp_type.__name__
    claimed = _DEFAULT_BY_NAME.get(name) or _DECLARED_BY_NAME.get(name)
    if claimed is not None and claimed is not stamp_type:
        raise MessageBusError(
            f"stamp name {name!r} is already used by {claimed.__module__}.{claimed.__qualname__}",
        )
    if claimed is None:
        _DECLARED_BY_NAME[name] = stamp_type


def declared_stamps() -> tuple[type[StampInterface], ...]:
    """Return every stamp class declared with ``@as_stamp``, in declaration order."""
    return tuple(_DECLARED_BY_NAME.values())


def stamp_type_for(name: str) -> type[StampInterface] | None:
    """Return the stamp class travelling as ``name``: one the library ships, or one declared."""
    return _DEFAULT_BY_NAME.get(name) or _DECLARED_BY_NAME.get(name)
