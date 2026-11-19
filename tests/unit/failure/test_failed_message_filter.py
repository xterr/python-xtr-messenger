from __future__ import annotations

from datetime import UTC, datetime

import pytest

from tests.support.listing import ListedTransport
from xtr_messenger import Envelope, FailedMessageFilter, SentToFailureTransportStamp

pytestmark = pytest.mark.anyio

_JAN = datetime(2026, 1, 1, tzinfo=UTC)
_MAR = datetime(2026, 3, 1, tzinfo=UTC)


def _failed(message: object, at: datetime) -> Envelope:
    return Envelope(message, (SentToFailureTransportStamp("orders", at),))


def test_an_empty_filter_selects_everything() -> None:
    assert FailedMessageFilter().is_empty()
    assert FailedMessageFilter().matches(Envelope("x"))


def test_it_selects_by_dotted_class_name() -> None:
    selected = FailedMessageFilter(class_name="builtins.str")

    assert selected.matches(Envelope("x"))
    assert not selected.matches(Envelope(1))


def test_both_bounds_are_inclusive() -> None:
    selected = FailedMessageFilter(failed_after=_JAN, failed_before=_MAR)

    assert selected.matches(_failed("x", _JAN))
    assert selected.matches(_failed("x", _MAR))
    assert not selected.matches(_failed("x", datetime(2026, 4, 1, tzinfo=UTC)))


def test_a_message_with_no_failure_time_is_never_selected_by_a_window() -> None:
    assert not FailedMessageFilter(failed_after=_JAN).matches(Envelope("x"))


async def test_the_limit_bounds_what_is_selected() -> None:
    transport = ListedTransport(_failed("a", _JAN), _failed(1, _JAN), _failed("b", _JAN))
    selected = FailedMessageFilter(class_name="builtins.str")

    assert [e.message async for e in selected.select(transport, 1)] == ["a"]
    assert [e.message async for e in selected.select(transport)] == ["a", "b"]
    assert [e.message async for e in FailedMessageFilter().select(transport, 2)] == ["a", 1]
