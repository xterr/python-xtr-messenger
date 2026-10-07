"""Unit tests for :class:`xtr_messenger.bundle.MessengerBundle`."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, final
from uuid import UUID, uuid4

import pytest
from typing_extensions import override
from xtr_dependency_injection import (
    Bundle,
    CompilerPassInterface,
    ContainerBuilder,
    Kernel,
    NoConfig,
    PassStage,
    ServicesResetter,
    as_bundle,
    as_service,
)
from xtr_dependency_injection.exception import InvalidDefinitionError
from xtr_dependency_injection.integration.wireup import create_container
from xtr_dependency_injection.testing import assert_zero_config
from xtr_logging.bundle import LoggingBundle
from xtr_service_contracts import ResetInterface

from tests.fixtures.app_units.services import Journal
from xtr_messenger import (
    Envelope,
    HandlersLocator,
    InvalidMiddlewareArgumentsError,
    MessageBusConfig,
    MessageBusInterface,
    MiddlewareInterface,
    StackInterface,
    UnknownMiddlewareError,
    WorkerFactory,
    as_message,
    as_message_handler,
    as_middleware,
)
from xtr_messenger.bundle import MessengerBundle
from xtr_messenger.middleware import StackMiddleware
from xtr_messenger.middleware.middleware_registry import (
    MiddlewareRegistry,
    middleware_declared_on,
)

if TYPE_CHECKING:
    from xtr_service_contracts import ContainerInterface

    from xtr_messenger.middleware.middleware_arguments import MiddlewareEntry

pytestmark = pytest.mark.anyio


@as_message(name="tests.unit.bundle.job.v1")
@dataclass(frozen=True, slots=True)
class RunJob:
    job_id: UUID


@as_service()
@final
class UnitLedger:
    def __init__(self) -> None:
        self.done: list[UUID] = []


@as_message_handler(RunJob)
@final
class RunJobHandler:
    def __init__(self, ledger: UnitLedger) -> None:
        self._ledger = ledger

    async def __call__(self, message: RunJob) -> None:
        self._ledger.done.append(message.job_id)


HANDLER_CALLS: list[UUID] = []


@as_message_handler(RunJob, HandlersLocator())
async def sink(message: RunJob) -> None:
    HANDLER_CALLS.append(message.job_id)


async def test_zero_config_boots_and_shuts_down() -> None:
    await assert_zero_config(MessengerBundle)


async def test_shutdown_closes_nothing_when_no_transport_factory_was_built() -> None:
    """An application that never resolved one built none, so there is nothing to
    close and no adapter is imported to be asked. Which kernel's publishers a
    shutdown does close is proved in
    ``tests/integration/bundle/test_messenger_transport_factory.py``.
    """
    await MessengerBundle().shutdown()


async def test_the_bundle_wires_a_bus_with_no_transports_when_unconfigured() -> None:
    kernel = Kernel(
        MessengerBundle.__module__,
        env="test",
        bundles={MessengerBundle: {"all": True}},
        resources=(),
    )
    booted = await kernel.boot()
    try:
        container: ContainerInterface = booted.container
        bus = await container.get(MessageBusInterface)
        assert bus is not None
    finally:
        await booted.shutdown()


async def test_a_class_handler_runs_with_container_built_dependencies() -> None:
    # Importing the fixture app declares its handlers; only this test should.
    from tests.fixtures.app_messenger.handlers import Ledger  # noqa: PLC0415
    from tests.fixtures.app_messenger.messages import DoWork  # noqa: PLC0415

    kernel = Kernel("tests.fixtures.app_messenger", env="test")
    booted = await kernel.boot()
    try:
        container: ContainerInterface = booted.container
        bus = await container.get(MessageBusInterface)
        job_id = uuid4()

        _ = await bus.dispatch(Envelope(DoWork(job_id)))

        ledger = await container.get(Ledger)
        assert ledger.done == [job_id]
    finally:
        await booted.shutdown()


# A registry of its own: the bundle reads the name off the class, and the
# process-wide registry stays as other tests expect it.
_TRACKING = MiddlewareRegistry()


@as_middleware("tracking", registry=_TRACKING)
@final
class DeclaredTrackingMiddleware(MiddlewareInterface):
    """As_middleware-decorated so the bundle picks it up by autoconfiguration."""

    calls: int = 0

    @override
    async def handle(self, envelope: Envelope, stack: StackInterface) -> Envelope:
        type(self).calls += 1
        return await stack.next().handle(envelope, stack)


async def test_middleware_declared_by_name_is_resolved_from_the_container() -> None:
    kernel = Kernel(
        __name__,
        env="test",
        bundles={MessengerBundle: {"all": True}},
    )
    booted = await kernel.boot()
    try:
        container: ContainerInterface = booted.container
        resolved = await container.get(MiddlewareInterface, "tracking")
        assert isinstance(resolved, DeclaredTrackingMiddleware)
    finally:
        await booted.shutdown()


def test_as_middleware_records_the_name_on_the_class() -> None:
    """The registry sees the class and the reader returns its names."""
    assert _TRACKING["tracking"] is DeclaredTrackingMiddleware
    assert "tracking" in tuple(middleware_declared_on(DeclaredTrackingMiddleware))


async def test_the_worker_factory_is_wired_with_the_services_resetter() -> None:
    kernel = Kernel(
        MessengerBundle.__module__,
        env="test",
        bundles={MessengerBundle: {"all": True}},
        resources=(),
    )
    booted = await kernel.boot()
    try:
        container: ContainerInterface = booted.container
        factory = await container.get(WorkerFactory)
        resetter = await container.get(ServicesResetter)
        assert factory is not None
        assert isinstance(resetter, ResetInterface) or callable(getattr(resetter, "reset", None))
    finally:
        await booted.shutdown()


async def test_the_console_command_is_registered_only_when_console_is_active() -> None:
    kernel = Kernel(
        MessengerBundle.__module__,
        env="test",
        bundles={MessengerBundle: {"all": True}},
        resources=(),
    )
    compiled = kernel.build()
    # The console is an optional peer.
    from xtr_console.command import commands_declared_on  # noqa: PLC0415

    # Importing it declares the console's commands; only this test should.
    from xtr_messenger.command.consume import ConsumeMessagesCommand  # noqa: PLC0415

    del compiled
    declarations = tuple(commands_declared_on(ConsumeMessagesCommand))
    assert declarations, "the consume command should still declare itself for the console"
    # console bundle is not required with a plain MessengerBundle-only kernel


def _built_with(*middleware: MiddlewareEntry, logging: bool = False) -> None:
    _ = create_container(
        (MessengerBundle, LoggingBundle) if logging else (MessengerBundle,),
        configs=[MessageBusConfig(middleware=middleware)],
        env="test",
        scan=["tests.fixtures.app_units.services"],
    )


def test_a_name_given_arguments_must_be_registered() -> None:
    with pytest.raises(UnknownMiddlewareError) as raised:
        _built_with({"nowhere": {"x": 1}})

    assert raised.value.name == "nowhere"
    assert "labelled" in raised.value.known


def test_arguments_a_middleware_does_not_take_fail_the_build() -> None:
    with pytest.raises(InvalidDefinitionError, match="colour"):
        _built_with({"labelled": {"colour": "red"}})


async def test_an_entry_keeps_the_arguments_its_middleware_was_defined_with() -> None:
    container = create_container(
        (MessengerBundle, _TimesThreeBundle),
        configs=[MessageBusConfig(middleware=[{"labelled": {"label": "entry"}}])],
        env="test",
        scan=["tests.fixtures.app_units.services"],
    )
    try:
        built = await container.get(MiddlewareInterface, "labelled#0")
        journal = await container.get(Journal)
        _ = await built.handle(Envelope("a"), StackMiddleware(()))
    finally:
        await container.close()

    assert journal.middleware == [("entry", 3)]


@final
class _SetTimes(CompilerPassInterface):
    """What a bundle may do to a middleware it defines: set one of its arguments."""

    @override
    def process(self, builder: ContainerBuilder) -> None:
        _ = builder.find_definition(MiddlewareInterface, "labelled").set_argument("times", 3)


@final
@as_bundle("tests_times_three")
class _TimesThreeBundle(Bundle[NoConfig]):
    @override
    def build(self, builder: ContainerBuilder) -> None:
        builder.add_compiler_pass(_SetTimes(), stage=PassStage.BEFORE_OPTIMIZATION)


def test_middleware_a_factory_builds_cannot_be_given_arguments() -> None:
    with pytest.raises(InvalidMiddlewareArgumentsError, match="built by a factory"):
        _built_with({"logging": {}}, logging=True)
