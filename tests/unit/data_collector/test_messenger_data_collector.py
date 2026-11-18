from __future__ import annotations

import json
from typing import TYPE_CHECKING, cast

import anyio
import anyio.lowlevel
import pytest
from xtr_http_kernel.data_collector import ProfiledRequest

from xtr_messenger.data_collector import DispatchedMessage, MessengerDataCollector

if TYPE_CHECKING:
    from xtr_http_message_contracts import ServerRequestInterface

pytestmark = pytest.mark.anyio

# Neither method reads the request: everything comes from the trace.
_PROFILED = ProfiledRequest(
    request=cast("ServerRequestInterface", object()), status_code=200, headers={}, state={}
)


def _dispatched(*, handled: int = 0, sent: int = 0, failed: bool = False) -> DispatchedMessage:
    return DispatchedMessage(
        message_class="app.DoWork",
        message={},
        stamps_in=(),
        handled=tuple({"handler": f"h{index}", "result": {}} for index in range(handled)),
        sent=tuple({"sender": "app.Sender", "alias": f"t{index}"} for index in range(sent)),
        duration_ms=2.5,
        error={"class": "builtins.ValueError", "message": "x", "handlers": []} if failed else None,
    )


def test_the_collector_is_named_messenger() -> None:
    assert MessengerDataCollector().name == "messenger"


def test_nothing_is_collected_before_a_request_begins() -> None:
    collector = MessengerDataCollector()
    collector.trace.record(_dispatched())

    data = collector.collect(_PROFILED)

    assert data == {
        "messages": [],
        "count": 0,
        "handled": 0,
        "sent": 0,
        "failed": 0,
        "duration_ms": 0,
    }


def test_the_request_s_dispatches_are_counted_and_listed_in_order() -> None:
    collector = MessengerDataCollector()
    collector.begin_request()
    first, second = _dispatched(handled=2), _dispatched(sent=1, failed=True)
    collector.trace.record(first)
    collector.trace.record(second)

    data = collector.late_collect(collector.collect(_PROFILED), _PROFILED)

    assert data == {
        "messages": [first.to_data(), second.to_data()],
        "count": 2,
        "handled": 2,
        "sent": 1,
        "failed": 1,
        "duration_ms": 5.0,
    }
    assert json.loads(json.dumps(data)) == data


def test_a_dispatch_still_running_adds_no_duration() -> None:
    collector = MessengerDataCollector()
    collector.begin_request()
    collector.trace.record(DispatchedMessage(message_class="app.DoWork", message={}, stamps_in=()))

    assert collector.collect(_PROFILED)["duration_ms"] == 0


def test_beginning_a_request_starts_from_nothing() -> None:
    collector = MessengerDataCollector()
    collector.begin_request()
    collector.trace.record(_dispatched())

    collector.begin_request()

    assert collector.collect(_PROFILED)["count"] == 0


async def test_concurrent_requests_each_collect_only_their_own_dispatches() -> None:
    collector = MessengerDataCollector()
    counts: dict[int, object] = {}

    async def request(dispatches: int) -> None:
        collector.begin_request()
        for _ in range(dispatches):
            collector.trace.record(_dispatched())
            await anyio.lowlevel.checkpoint()
        counts[dispatches] = collector.collect(_PROFILED)["count"]

    async with anyio.create_task_group() as group:
        _ = group.start_soon(request, 1)
        _ = group.start_soon(request, 3)

    assert counts == {1: 1, 3: 3}
