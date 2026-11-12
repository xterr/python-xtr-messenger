"""Turning an error into text, whatever the error does."""

from __future__ import annotations

from tests.support.errors import UnprintableError
from xtr_messenger._text import safe_str


def test_an_error_reads_as_it_prints_itself() -> None:
    assert safe_str(ValueError("boom")) == "boom"


def test_an_error_that_cannot_print_itself_is_named_by_its_class() -> None:
    assert safe_str(UnprintableError()) == "[unprintable tests.support.errors.UnprintableError]"
