"""Console commands for a messenger process, on xtr-console.

Needs xtr-console installed beside this package. Importing this package
declares ``messenger:consume`` with ``@as_command``, like any other command
module.

The :class:`~xtr_messenger.bundle.MessengerBundle` loads this module for its
kernel when the console bundle is active, so the container builds the command
from the ``WorkerFactory`` the messenger bundle provides.

Run without a container, the console cannot fill the command's ``WorkerFactory``
and reports the missing parameter rather than build it bare.
"""

from __future__ import annotations

from .consume import ConsumeMessagesCommand
from .debug_command import DebugCommand
from .failed_messages_remove_command import FailedMessagesRemoveCommand
from .failed_messages_retry_command import FailedMessagesRetryCommand
from .failed_messages_show_command import FailedMessagesShowCommand
from .setup_transports_command import SetupTransportsCommand
from .show_messages_command import ShowMessagesCommand
from .stats_command import StatsCommand

__all__ = [
    "ConsumeMessagesCommand",
    "DebugCommand",
    "FailedMessagesRemoveCommand",
    "FailedMessagesRetryCommand",
    "FailedMessagesShowCommand",
    "SetupTransportsCommand",
    "ShowMessagesCommand",
    "StatsCommand",
]
