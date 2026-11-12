"""Errors a test hands the library, to see what it makes of them."""

from __future__ import annotations

from typing import final

from typing_extensions import override


@final
class UnprintableError(Exception):
    """An error whose ``__str__`` raises, as one built around a broken message does."""

    @override
    def __str__(self) -> str:
        raise TypeError("sequence item 0: expected str instance, NoneType found")
