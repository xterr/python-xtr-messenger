"""The envelope — a message plus the stamps it accumulated in transit."""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, TypeVar

from .stamp import StampInterface

if TYPE_CHECKING:
    from collections.abc import Iterable

__all__ = ["Envelope"]

StampT = TypeVar("StampT", bound=StampInterface)


@dataclass(frozen=True, slots=True)
class Envelope:
    """A dispatched message wrapped with an ordered tuple of stamps.

    The envelope is immutable: every mutator returns a new instance, so a
    middleware can hand the same envelope down the chain while publishing its
    own stamped variant upstream, and two senders in a fan-out never observe
    each other's stamps.

    Attributes:
        message: The payload being dispatched. The bus never inspects it.
        stamps: Stamps appended by the middleware traversed so far, in order.
    """

    message: object
    stamps: tuple[StampInterface, ...] = ()

    @classmethod
    def wrap(cls, message: object, stamps: Iterable[StampInterface] = ()) -> Envelope:
        """Return ``message`` as an envelope, passing an existing one through.

        An already-wrapped envelope keeps its stamps and gains ``stamps``,
        which is what makes re-dispatching a stamped envelope meaningful.
        """
        extra = tuple(stamps)
        if isinstance(message, Envelope):
            return message.with_stamps(*extra) if extra else message
        return cls(message, extra)

    def with_stamps(self, *stamps: StampInterface) -> Envelope:
        """Return a copy of this envelope with ``stamps`` appended."""
        if not stamps:
            return self
        return replace(self, stamps=(*self.stamps, *stamps))

    def without_stamps(self, stamp_type: type[StampInterface]) -> Envelope:
        """Return a copy with every ``stamp_type`` stamp removed.

        Subclasses are removed too, which is how a serializer drops the whole
        :class:`~xtr_messenger.stamp.NonSendableStampInterface` family in one call.
        """
        kept = tuple(stamp for stamp in self.stamps if not isinstance(stamp, stamp_type))
        if len(kept) == len(self.stamps):
            return self
        return replace(self, stamps=kept)

    def all(self, stamp_type: type[StampT]) -> tuple[StampT, ...]:
        """Return every stamp of ``stamp_type``, in the order they were added."""
        return tuple(stamp for stamp in self.stamps if isinstance(stamp, stamp_type))

    def last(self, stamp_type: type[StampT]) -> StampT | None:
        """Return the most recently added ``stamp_type`` stamp, or ``None``.

        When a stamp type can be added more than once, the last one wins.
        """
        matches = self.all(stamp_type)
        return matches[-1] if matches else None
