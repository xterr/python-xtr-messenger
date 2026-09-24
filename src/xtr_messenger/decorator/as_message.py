"""Declaring a class as a message.

``@as_message`` marks a class as something the bus carries and declares its
properties in one place, on the class itself.

Two properties are supported:

``name``
    The identity the message travels under. It is the contract between
    producer and consumer — both import the message class, so neither needs
    to import the other's code, and a producer's import graph never drags in
    handlers or a database driver. Defaults to ``module:QualName``, which is
    convenient but changes if the class moves, so anything that outlives a
    deploy should pin an explicit, versioned name.

``transport``
    Where the message goes when no routing table entry matches. A routing
    table always wins; this is the fallback, so a message can carry a sane
    default without every application repeating it.

::

    @as_message(name="ingest.document.v1", transport="async")
    @dataclass(frozen=True, slots=True)
    class IngestDocument:
        document_id: UUID
"""

from __future__ import annotations

from typing import TYPE_CHECKING, TypeVar, overload

from xtr_messenger.message_registry import register_message

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

__all__ = ["as_message"]

MessageT = TypeVar("MessageT", bound=type)


@overload
def as_message(message_type: MessageT, /) -> MessageT: ...


@overload
def as_message(
    *,
    name: str | None = ...,
    transport: str | Sequence[str] | None = ...,
) -> Callable[[MessageT], MessageT]: ...


def as_message(
    message_type: MessageT | None = None,
    /,
    *,
    name: str | None = None,
    transport: str | Sequence[str] | None = None,
) -> MessageT | Callable[[MessageT], MessageT]:
    """Declare a class as a message, with its wire name and default transport.

    Every property is optional, and so are the parentheses::

        @as_message                                   # marker only
        @as_message()                                 # the same
        @as_message(name="ingest.document.v1")        # identity only
        @as_message(transport="async")                # default routing only
        @as_message(name="...", transport="async")    # both

    With no ``name`` the message travels as ``module:QualName``; with no
    ``transport`` it goes wherever the routing table sends it.

    Raises:
        MessageBusError: If ``name`` is already claimed by a different class.
    """
    transports = _as_tuple(transport)

    def declare(target: MessageT) -> MessageT:
        register_message(target, name, transports)
        return target

    return declare if message_type is None else declare(message_type)


def _as_tuple(transport: str | Sequence[str] | None) -> tuple[str, ...]:
    if transport is None:
        return ()
    if isinstance(transport, str):
        return (transport,)
    return tuple(transport)
