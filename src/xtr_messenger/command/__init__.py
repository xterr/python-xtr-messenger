"""Console commands for a messenger process, on xtr-console.

Install with the ``console`` extra. Importing this package declares
``messenger:consume`` with ``@as_command``, like any other command module.

With a wireup container, import it before the console's ``injectables()``,
and the container builds the command from the ``WorkerFactory`` the
messenger's ``injectables()`` provide — handlers wired::

    import xtr_messenger.command  # noqa: F401

    container = wireup.create_async_container(
        injectables=[
            *messenger.injectables(CONFIG),
            *console.injectables(Application("acme")),
        ],
    )

Without one, tell the command which factory to build workers with::

    from xtr_messenger.command import ConsumeMessagesCommand

    ConsumeMessagesCommand.use_workers(WorkerFactory(CONFIG))
    raise SystemExit(Application("acme").run())
"""

from .consume import ConsumeMessagesCommand

__all__ = ["ConsumeMessagesCommand"]
