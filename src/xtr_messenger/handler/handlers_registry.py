"""Message-handler declarations discovered on scanned objects.

:func:`~xtr_messenger.decorator.as_message_handler` records the message
types a function or class handles on the object itself; the messenger bundle
reads those with :func:`handlers_declared_on` — a reader for
:meth:`~xtr_dependency_injection.builder.ContainerBuilder.register_attribute_for_autoconfiguration`
— so its per-kernel :class:`HandlersLocator` sees every declared handler.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

if TYPE_CHECKING:
    from collections.abc import Iterable

__all__ = ["HANDLERS_ATTRIBUTE", "handlers_declared_on"]

HANDLERS_ATTRIBUTE = "__xtr_messenger_message_handlers__"
"""Where :func:`~xtr_messenger.decorator.as_message_handler` records message types."""


def handlers_declared_on(obj: object) -> Iterable[type]:
    """Yield every message type declared on ``obj`` by ``@as_message_handler``.

    Both function and class handlers carry the attribute; a message type
    appearing more than once (a repeated decorator) is yielded once.
    """
    declarations: object = getattr(obj, HANDLERS_ATTRIBUTE, ())
    if not isinstance(declarations, tuple):
        return ()
    typed = cast("tuple[object, ...]", declarations)
    seen: dict[type, None] = {}
    for entry in typed:
        if isinstance(entry, type) and entry not in seen:
            seen[entry] = None
    return tuple(seen)
