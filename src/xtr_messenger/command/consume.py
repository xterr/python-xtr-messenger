"""``messenger:consume``: a worker for the transports named on the command line."""

from __future__ import annotations

import asyncio
import signal
from typing import Annotated, ClassVar, Final, final

from cyclopts import Parameter, validators
from rich.markup import escape
from xtr_console import ConsoleStyle, ExitCode, as_command

from xtr_messenger.exception import MessageBusError
from xtr_messenger.message_bus_config import MessageBusConfig
from xtr_messenger.worker_factory import WorkerFactory
from xtr_messenger.worker_interface import WorkerInterface

__all__ = ["ConsumeMessagesCommand"]

# Typed as what a container fills the parameter with, so the engine still
# matches it; ``WorkerFactory | None`` is a different type and never would be.
_UNSET: Final = WorkerFactory(MessageBusConfig(transports={}))

_NO_WORKERS: Final = (
    "No worker factory: wire a container, or call ConsumeMessagesCommand.use_workers()."
)


@as_command("messenger:consume")
@final
class ConsumeMessagesCommand:
    """Consumes the transports named on the command line, until stopped.

    An xtr-dependency-injection container builds it with the ``WorkerFactory``
    the messenger bundle provides. Without one, the console builds it bare,
    and it uses the factory given to :meth:`use_workers`.
    """

    __slots__ = ("_workers",)

    _process_workers: ClassVar[WorkerFactory | None] = None

    def __init__(self, workers: WorkerFactory = _UNSET) -> None:
        """Build workers with ``workers``, or with :meth:`use_workers`' when omitted."""
        self._workers = workers

    @classmethod
    def use_workers(cls, workers: WorkerFactory | None) -> None:
        """Build workers with ``workers`` wherever no container supplies one."""
        cls._process_workers = workers

    async def __call__(
        self,
        io: ConsoleStyle,
        *transports: str,
        time_limit: Annotated[float | None, Parameter(validator=validators.Number(gt=0))] = None,
    ) -> int:
        """Consume messages from the named transports.

        SIGTERM stops the worker once the message in hand is settled;
        Ctrl-C cancels it.

        Args:
            io: Where the command writes.
            transports: The transports to consume, as named in the configuration.
            time_limit: Stop, the same way, after this many seconds.
        """
        workers = self._workers if self._workers is not _UNSET else self._process_workers
        if workers is None:
            io.error(_NO_WORKERS)
            return ExitCode.FAILURE
        if not transports:
            io.error("Name at least one transport to consume.")
            return ExitCode.INVALID
        try:
            worker = workers.worker(transports)
        except MessageBusError as error:
            io.error(escape(str(error)))
            return ExitCode.FAILURE
        io.success(f"Consuming messages from {escape(', '.join(transports))}.")
        io.note("Quit the worker with CONTROL-C.")
        await _run(worker, time_limit)
        return ExitCode.SUCCESS


async def _run(worker: WorkerInterface, time_limit: float | None) -> None:
    """Run ``worker`` until it returns, is stopped by SIGTERM, or runs out of time."""
    loop = asyncio.get_running_loop()
    timer = loop.call_later(time_limit, worker.stop) if time_limit is not None else None
    on_sigterm = _stop_on_sigterm(loop, worker)
    try:
        await worker.run()
    finally:
        if timer is not None:
            timer.cancel()
        if on_sigterm:
            _ = loop.remove_signal_handler(signal.SIGTERM)


def _stop_on_sigterm(loop: asyncio.AbstractEventLoop, worker: WorkerInterface) -> bool:
    """Have SIGTERM stop ``worker``; report whether the loop could arrange it.

    It cannot on Windows, nor outside the main thread; the worker then stops
    only when cancelled.
    """
    try:
        loop.add_signal_handler(signal.SIGTERM, worker.stop)
    except (NotImplementedError, RuntimeError):
        return False
    return True
