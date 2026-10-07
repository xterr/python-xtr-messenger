"""Import-cost claims for the bundle's shared transport factory.

The factory the container builds must discover adapters by DSN scheme, on
demand — building it eagerly imported every advertised adapter, broker
libraries included, in applications that never spoke to a broker. Proving a
module was *never* imported needs a fresh interpreter, so these run in a
subprocess.
"""

from __future__ import annotations

import subprocess
import sys


def test_the_bundles_factory_never_imports_a_broker_library_for_sync_only() -> None:
    code = (
        "from xtr_messenger import TransportConfig\n"
        "from xtr_messenger.bundle._services import (\n"
        "    BuiltTransports, combined_transport_factory,\n"
        ")\n"
        "factory = combined_transport_factory(BuiltTransports())\n"
        "senders = factory.create({'s': TransportConfig('sync://')})\n"
        "assert senders\n"
        "import sys\n"
        "assert 'taskiq' not in sys.modules, 'taskiq imported for a sync-only app'\n"
        "assert 'aio_pika' not in sys.modules, 'aio_pika imported for a sync-only app'\n"
    )

    result = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, check=False
    )

    assert result.returncode == 0, result.stderr


def test_registered_factories_still_come_ahead_of_discovery_without_eager_imports() -> None:
    """The variant that puts app-registered factories first keeps discovery
    lazy too: the registered factory serves its scheme, and no broker library
    is imported for it."""
    code = (
        "from xtr_messenger import Dsn, TransportConfig, TransportFactoryInterface\n"
        "from xtr_messenger.bundle._services import (\n"
        "    BuiltTransports, combined_transport_factory_with,\n"
        ")\n"
        "class Mine(TransportFactoryInterface):\n"
        "    def supports(self, dsn: Dsn) -> bool:\n"
        "        return dsn.scheme == 'sync'\n"
        "    def create(self, group):\n"
        "        return {name: object() for name in group}\n"
        "mine = Mine()\n"
        "factory = combined_transport_factory_with([mine], BuiltTransports())\n"
        "assert factory.serving({'s': TransportConfig('sync://')}) is mine\n"
        "import sys\n"
        "assert 'taskiq' not in sys.modules, 'taskiq imported for a sync-only app'\n"
    )

    result = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, check=False
    )

    assert result.returncode == 0, result.stderr
