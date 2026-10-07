"""Console commands for a messenger process, on xtr-console.

Install with the ``console`` extra. Importing this package declares
``messenger:consume`` with ``@as_command``, like any other command module.

The :class:`~xtr_messenger.bundle.MessengerBundle` loads this module for its
kernel when the console bundle is active, so the container builds the command
from the ``WorkerFactory`` the messenger bundle provides.

Run without a container, the console cannot fill the command's ``WorkerFactory``
and reports the missing parameter rather than build it bare.
"""

from __future__ import annotations

from .consume import ConsumeMessagesCommand

__all__ = ["ConsumeMessagesCommand"]
