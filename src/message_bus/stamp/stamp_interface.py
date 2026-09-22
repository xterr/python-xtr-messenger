"""The contract every envelope stamp implements."""

from __future__ import annotations

from dataclasses import dataclass

__all__ = ["StampInterface"]


@dataclass(frozen=True, slots=True)
class StampInterface:
    """Marker every stamp implements by inheriting it.

    Carries no data of its own — it exists so ``Envelope.all`` and
    ``Envelope.last`` can filter by type. Stamps stay frozen: they are value
    objects exchanged between middleware, and mutability would let one
    middleware rewrite another's recorded result.

    Deliberately a base class rather than a ``typing.Protocol``. A protocol
    with no members is structurally satisfied by *every* object — including
    ``None`` and ``42`` — so ``isinstance`` against it is always true, and
    stripping by stamp family would silently discard every stamp.
    Inheritance is what makes the check mean something.
    """
