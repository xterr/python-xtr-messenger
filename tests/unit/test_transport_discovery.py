from __future__ import annotations

import pathlib
import subprocess
import sys

import pytest

from message_bus import (
    Dsn,
    MessageBusConfig,
    MessageBusFactory,
    TransportConfig,
    UnsupportedDsnError,
)
from message_bus.transport import transport_factory_discovery as discovery
from message_bus.transport.transport_factory_discovery import (
    ENTRY_POINT_GROUP,
    advertised_schemes,
    default_factories,
    factory_for,
)


def test_every_builtin_scheme_is_advertised() -> None:
    assert {"sync", "in-memory", "amqp"} <= set(advertised_schemes())


def test_a_scheme_resolves_to_the_factory_that_serves_it() -> None:
    factory = factory_for(Dsn.parse("sync://"))

    assert factory is not None
    assert type(factory).__name__ == "SyncTransportFactory"


def test_an_unadvertised_scheme_resolves_to_nothing() -> None:
    assert factory_for(Dsn.parse("carrier-pigeon://")) is None


def test_the_group_name_is_part_of_the_public_contract() -> None:
    """The one string a third-party adapter has to hard-code.

    Deliberately not tied to the module that defines it: that module has been
    renamed twice while this has not, and twice a mechanical rename tried to
    drag it along.
    """
    assert ENTRY_POINT_GROUP == "message_bus.transport_factories"


def test_the_documented_group_name_matches_the_one_actually_used() -> None:
    """The docstring is where a third party copies the group name from.

    A rename reaching the prose but not the constant yields adapters that
    register against nothing — with no error anywhere to say so.
    """
    documented = discovery.__doc__ or ""

    assert f'[project.entry-points."{ENTRY_POINT_GROUP}"]' in documented
    assert "transport_factory_locator" not in documented


def test_the_packaged_entry_points_use_the_group_the_code_reads() -> None:
    """pyproject.toml and the constant must agree, or nothing resolves."""
    manifest = (pathlib.Path(__file__).resolve().parents[2] / "pyproject.toml").read_text()

    assert f'[project.entry-points."{ENTRY_POINT_GROUP}"]' in manifest


def test_default_factories_does_not_repeat_a_factory_serving_two_schemes() -> None:
    names = [type(f).__name__ for f in default_factories()]

    assert len(names) == len(set(names))


def test_resolving_a_sync_bus_never_imports_a_broker_library() -> None:
    """Lazy resolution: an app that only speaks sync:// must not pay for AMQP."""
    code = (
        "import asyncio\n"
        "from dataclasses import dataclass\n"
        "from uuid import UUID, uuid4\n"
        "from message_bus import MessageBusConfig, MessageBusFactory, TransportConfig, as_message\n"
        "@as_message(name='discovery.probe.v1')\n"
        "@dataclass(frozen=True, slots=True)\n"
        "class Probe:\n"
        "    identifier: UUID\n"
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


def test_an_explicit_factory_list_bypasses_discovery_entirely() -> None:
    config = MessageBusConfig(transports={"sync": TransportConfig("sync://")})

    with pytest.raises(UnsupportedDsnError, match="no transport factory"):
        _ = MessageBusFactory(config, []).bus()
