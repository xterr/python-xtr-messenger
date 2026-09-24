"""Import-cost claims: a scheme's driver is imported only when that scheme is used.

These need a fresh interpreter to prove a module was *never* imported, so they
run in a subprocess rather than as ordinary unit tests.
"""

from __future__ import annotations

import subprocess
import sys


def test_resolving_a_sync_bus_never_imports_a_broker_library() -> None:
    """Lazy resolution: an app that only speaks sync:// must not pay for AMQP.

    A sync-routed message needs a declared handler now, or the bus's own
    handling step raises NoHandlerForMessageError.
    """
    code = (
        "import asyncio\n"
        "from dataclasses import dataclass\n"
        "from uuid import UUID, uuid4\n"
        "from message_bus import MessageBusConfig, MessageBusFactory, TransportConfig\n"
        "from message_bus import as_message, as_message_handler\n"
        "@as_message(name='test.integration.transport.probe.v1')\n"
        "@dataclass(frozen=True, slots=True)\n"
        "class Probe:\n"
        "    identifier: UUID\n"
        "@as_message_handler(Probe)\n"
        "async def handle(message: Probe) -> None: ...\n"
        "config = MessageBusConfig(\n"
        "    transports={'sync': TransportConfig('sync://')}, routing={Probe: 'sync'}\n"
        ")\n"
        "asyncio.run(MessageBusFactory(config).bus().dispatch(Probe(identifier=uuid4())))\n"
        "import sys\n"
        "assert 'taskiq' not in sys.modules, 'taskiq imported for a sync-only app'\n"
        "assert 'aio_pika' not in sys.modules, 'aio_pika imported for a sync-only app'\n"
    )

    result = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, check=False
    )

    assert result.returncode == 0, result.stderr


def test_an_amqp_bus_does_load_its_broker_library() -> None:
    code = (
        "from message_bus import MessageBusConfig, MessageBusFactory, TransportConfig\n"
        "config = MessageBusConfig(\n"
        "    transports={'q': TransportConfig('amqp://guest:guest@h:5672/', queue='jobs')}\n"
        ")\n"
        "MessageBusFactory(config).bus()\n"
        "import sys\n"
        "assert 'taskiq' in sys.modules\n"
    )

    result = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, check=False
    )

    assert result.returncode == 0, result.stderr
