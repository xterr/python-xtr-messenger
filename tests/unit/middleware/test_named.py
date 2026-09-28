"""The chain a configuration describes, and what its names mean."""

from __future__ import annotations

import pytest
from typing_extensions import override
from xtr_logging import Logger, TestHandler
from xtr_logging_contracts import Level

from tests.support.fakes import OneStep, RecordingMiddleware
from tests.support.messages import ingest_document
from xtr_messenger import (
    Envelope,
    LoggingMiddleware,
    MessageBusConfig,
    MiddlewareInterface,
    StackInterface,
    UnknownMiddlewareError,
)
from xtr_messenger.decorator import as_middleware
from xtr_messenger.middleware.middleware_registry import MiddlewareRegistry
from xtr_messenger.middleware.named import chain, named_middleware, resolve

pytestmark = pytest.mark.anyio


def a_config(*middleware: str | MiddlewareInterface, defaults: bool = True) -> MessageBusConfig:
    return MessageBusConfig(transports={}, middleware=middleware, default_middleware=defaults)


def test_nothing_configured_builds_nothing() -> None:
    assert resolve((), named_middleware()) == ()


def test_logging_is_the_one_name_the_library_ships() -> None:
    assert tuple(named_middleware()) == ("logging",)


def test_a_name_is_built() -> None:
    [built] = resolve(["logging"], named_middleware())

    assert isinstance(built, LoggingMiddleware)


async def test_the_logging_name_writes_through_the_logger_it_is_given() -> None:
    handler = TestHandler()
    [built] = resolve(["logging"], named_middleware(Logger("app", [handler])))

    _ = await built.handle(Envelope(ingest_document()), OneStep())

    assert handler.has_record("message dispatched", Level.NOTICE)


def test_an_instance_passes_straight_through() -> None:
    mine = RecordingMiddleware()

    assert resolve([mine], named_middleware()) == (mine,)


def test_order_is_kept_across_names_and_instances() -> None:
    mine = RecordingMiddleware()

    built = resolve([mine, "logging"], named_middleware())

    assert built[0] is mine
    assert isinstance(built[1], LoggingMiddleware)


def test_an_unknown_name_is_refused_naming_what_is_registered() -> None:
    with pytest.raises(UnknownMiddlewareError) as excinfo:
        _ = resolve(["nope"], named_middleware())

    assert excinfo.value.name == "nope"
    assert excinfo.value.known == ("logging",)


def test_a_name_of_your_own_is_added_alongside_the_librarys() -> None:
    mine = RecordingMiddleware()

    named = named_middleware(named={"audit": lambda: mine})

    assert set(named) == {"logging", "audit"}
    assert resolve(["audit"], named) == (mine,)


def test_a_name_of_your_own_replaces_the_librarys() -> None:
    mine = RecordingMiddleware()

    assert resolve(["logging"], named_middleware(named={"logging": lambda: mine})) == (mine,)


def test_the_chain_is_what_is_named_then_the_defaults() -> None:
    mine, tail = RecordingMiddleware(), RecordingMiddleware()

    assert chain(a_config(mine), named_middleware(), lambda: [tail]) == [mine, tail]


def test_default_middleware_off_never_builds_the_defaults() -> None:
    """They may open a connection, so they must not even be built."""
    mine = RecordingMiddleware()

    def defaults() -> list[MiddlewareInterface]:
        raise AssertionError

    assert chain(a_config(mine, defaults=False), named_middleware(), defaults) == [mine]


class _Passing(MiddlewareInterface):
    """Hands the envelope on; a base for classes declared by name in these tests."""

    @override
    async def handle(self, envelope: Envelope, stack: StackInterface, /) -> Envelope:
        return await stack.next().handle(envelope, stack)


def test_a_class_declared_by_name_is_built_where_a_chain_names_it() -> None:
    registry = MiddlewareRegistry()

    @as_middleware("recording", registry=registry)
    class Declared(_Passing):
        pass

    [built] = resolve(["recording"], named_middleware(registry=registry))

    assert isinstance(built, Declared)


def test_a_class_declared_after_the_names_were_read_still_resolves() -> None:
    registry = MiddlewareRegistry()
    named = named_middleware(registry=registry)

    @as_middleware("late", registry=registry)
    class Late(_Passing):
        pass

    [built] = resolve(["late"], named)

    assert isinstance(built, Late)


def test_a_name_given_wins_over_one_declared() -> None:
    registry = MiddlewareRegistry()
    mine = RecordingMiddleware()

    @as_middleware("audit", registry=registry)
    class Declared(_Passing):
        pass

    named = named_middleware(named={"audit": lambda: mine}, registry=registry)

    assert resolve(["audit"], named) == (mine,)
