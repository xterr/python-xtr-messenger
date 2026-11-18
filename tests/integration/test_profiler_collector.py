"""The messenger panel through a real kernel: there with the profiler on, absent with it off."""

from __future__ import annotations

from typing import TYPE_CHECKING, cast
from uuid import uuid4

import pytest
from xtr_dependency_injection import Kernel
from xtr_http_kernel.bundle import HttpKernelBundle
from xtr_http_kernel.data_collector import DataCollectorTemplates, ProfiledRequest
from xtr_http_kernel.profiler import Profiler

from tests.fixtures.app_messenger.messages import AuditEvent, DoWork
from xtr_messenger import MessageBusInterface
from xtr_messenger.bundle import MessengerBundle
from xtr_messenger.data_collector import MessengerDataCollector, TraceableMessageBus

if TYPE_CHECKING:
    from pathlib import Path

    from xtr_http_message_contracts import ServerRequestInterface

pytestmark = pytest.mark.anyio

_PROFILED = ProfiledRequest(
    request=cast("ServerRequestInterface", object()), status_code=200, headers={}, state={}
)


@pytest.fixture(autouse=True)
def _folder(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PROFILER_TEST_FOLDER", str(tmp_path))


def _kernel(*resources: str) -> Kernel:
    return Kernel(
        "tests.fixtures.app_messenger",
        env="test",
        bundles={MessengerBundle: {"all": True}, HttpKernelBundle: {"all": True}},
        resources=("tests.fixtures.app_messenger", *resources),
    )


async def test_the_collector_joins_the_profiler_when_profiling_is_on() -> None:
    async with await _kernel("tests.fixtures.profiler_on").boot() as booted:
        profiler = await booted.container.get(Profiler)
        templates = await booted.container.get(DataCollectorTemplates)
        bus = await booted.container.get(MessageBusInterface)

    assert profiler.has("messenger")
    assert templates.template_of("messenger") == "@xtr_messenger/collector/messenger.html.j2"
    assert isinstance(bus, TraceableMessageBus)


async def test_a_profiled_request_s_dispatches_are_collected() -> None:
    async with await _kernel("tests.fixtures.profiler_on").boot() as booted:
        collector = await booted.container.get(MessengerDataCollector)
        bus = await booted.container.get(MessageBusInterface)
        collector.begin_request()

        _ = await bus.dispatch(DoWork(uuid4()))
        _ = await bus.dispatch(AuditEvent("signup"))

        data = collector.late_collect(collector.collect(_PROFILED), _PROFILED)

    messages = cast("list[dict[str, object]]", data["messages"])
    assert [message["message_class"] for message in messages] == [
        "tests.fixtures.app_messenger.messages.DoWork",
        "tests.fixtures.app_messenger.messages.AuditEvent",
    ]
    assert (data["count"], data["handled"], data["sent"], data["failed"]) == (2, 2, 2, 0)
    assert messages[0]["sent"] == [{"sender": "SyncTransport", "alias": "sync"}]
    assert messages[1]["sent"] == [{"sender": "InMemoryTransport", "alias": "jobs"}]
    assert all(isinstance(message["duration_ms"], float) for message in messages)


async def test_the_collector_is_not_registered_when_profiling_is_off() -> None:
    async with await _kernel().boot() as booted:
        registered = booted.container.has(MessengerDataCollector)
        bus = await booted.container.get(MessageBusInterface)

    assert not registered
    assert not isinstance(bus, TraceableMessageBus)
