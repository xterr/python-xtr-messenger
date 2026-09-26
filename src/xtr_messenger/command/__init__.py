"""Console commands for a messenger process, on xtr-console.

Install with the ``console`` extra. Importing this package declares
``messenger:consume`` with ``@as_command``, like any other command module.

The :class:`~xtr_messenger.bundle.MessengerBundle` loads this module for its
kernel when the console bundle is active, so the container builds the command
from the ``WorkerFactory`` the messenger bundle provides.

Without a kernel, tell the command which factory to build workers with::

    from xtr_messenger.command import ConsumeMessagesCommand

    ConsumeMessagesCommand.use_workers(WorkerFactory(CONFIG))
    raise SystemExit(Application("acme").run())
"""

from .consume import ConsumeMessagesCommand

__all__ = ["ConsumeMessagesCommand"]
