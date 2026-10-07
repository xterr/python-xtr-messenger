"""``messenger:consume``: a worker for the transports named on the command line."""

from __future__ import annotations

import asyncio
import signal
from typing import TYPE_CHECKING, Annotated, final

from xtr_console import ConsoleStyle, ExitCode, Option, Range, as_command, escape

from xtr_messenger.exception import MessageBusError
from xtr_messenger.worker_factory import WorkerFactory
from xtr_messenger.worker_interface import WorkerInterface

if TYPE_CHECKING:
    from collections.abc import Callable
    from types import FrameType
    from typing import TypeAlias

    #: What :func:`signal.getsignal` returns, and what :func:`signal.signal`
    #: accepts — the handler object a restore puts back.
    _SignalHandler: TypeAlias = Callable[[int, FrameType | None], object] | int | None

__all__ = ["ConsumeMessagesCommand"]


@as_command("messenger:consume")
@final
class ConsumeMessagesCommand:
    """Consumes the transports named on the command line, until stopped.

    An xtr-dependency-injection container builds it with the ``WorkerFactory``
    the messenger bundle provides. Run without a container, the console cannot
    fill ``workers`` and reports the missing parameter rather than build it bare.
    """

    __slots__ = ("_workers",)

    def __init__(self, workers: WorkerFactory) -> None:
        """Build workers with ``workers``, which a container supplies."""
        self._workers = workers

    async def __call__(
        self,
        io: ConsoleStyle,
        *transports: str,
        time_limit: Annotated[float | None, Option(validator=Range(gt=0))] = None,
    ) -> int:
        """Consume messages from the named transports.

        SIGTERM stops the worker once the message in hand is settled;
        Ctrl-C cancels it.

        Args:
            io: Where the command writes.
            transports: The transports to consume, as named in the configuration.
            time_limit: Stop, the same way, after this many seconds.
        """
        if not transports:
            io.error("Name at least one transport to consume.")
            return ExitCode.INVALID
        try:
            worker = self._workers.worker(transports)
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
    arranged, previous = _stop_on_sigterm(loop, worker)
    try:
        await worker.run()
    finally:
        if timer is not None:
            timer.cancel()
        if arranged:
            _restore_sigterm(loop, previous)


def _stop_on_sigterm(
    loop: asyncio.AbstractEventLoop, worker: WorkerInterface
) -> tuple[bool, _SignalHandler]:
    """Have SIGTERM stop ``worker``; report it, and the handler it displaced.

    The first element is ``False`` when the loop cannot handle signals —
    Windows, or outside the main thread — and the worker then stops only when
    cancelled. When ``True``, the second element is the SIGTERM handler that was
    installed before, so :func:`_restore_sigterm` can put it back rather than
    leave the default, which would silently drop an application's own handler.
    """
    try:
        previous = signal.getsignal(signal.SIGTERM)
    except ValueError:
        return (False, None)
    try:
        loop.add_signal_handler(signal.SIGTERM, worker.stop)
    except (NotImplementedError, RuntimeError):
        return (False, None)
    return (True, previous)


def _restore_sigterm(
    loop: asyncio.AbstractEventLoop,
    previous: _SignalHandler,
) -> None:
    """Undo :func:`_stop_on_sigterm`, putting the displaced handler back.

    A handler installed from C has no Python object to restore — ``getsignal``
    reports it as ``None`` — so the loop's own removal, which leaves the
    default, is as close as can be got.
    """
    _ = loop.remove_signal_handler(signal.SIGTERM)
    if previous is not None:
        _ = signal.signal(signal.SIGTERM, previous)
