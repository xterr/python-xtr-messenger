from __future__ import annotations

import pathlib

from xtr_messenger import Dsn
from xtr_messenger.transport import transport_factory_discovery as discovery
from xtr_messenger.transport.transport_factory_discovery import (
    ENTRY_POINT_GROUP,
    advertised_schemes,
    default_factories,
    factory_for,
)

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[3]


def test_every_builtin_scheme_is_advertised() -> None:
    assert {"sync", "in-memory", "amqp"} <= set(advertised_schemes())


def test_a_scheme_resolves_to_the_factory_that_serves_it() -> None:
    factory = factory_for(Dsn.parse("sync://"))

    assert factory is not None
    assert type(factory).__name__ == "SyncTransportFactory"


def test_an_unadvertised_scheme_resolves_to_nothing() -> None:
    assert factory_for(Dsn.parse("carrier-pigeon://")) is None


def test_default_factories_does_not_repeat_a_factory_serving_two_schemes() -> None:
    """amqp and amqps share one factory, which must be listed once."""
    names = [type(f).__name__ for f in default_factories()]

    assert len(names) == len(set(names))


def test_default_factories_includes_the_builtin_factories() -> None:
    names = {type(f).__name__ for f in default_factories()}

    assert {"SyncTransportFactory", "InMemoryTransportFactory"} <= names


def test_the_group_name_is_part_of_the_public_contract() -> None:
    """The one string a third-party adapter has to hard-code.

    Deliberately not tied to the module that defines it: that module has been
    renamed twice while this has not.
    """
    assert ENTRY_POINT_GROUP == "xtr_messenger.transport_factories"


def test_the_documented_group_name_matches_the_one_actually_used() -> None:
    """The docstring is where a third party copies the group name from.

    A rename reaching the prose but not the constant yields adapters that
    register against nothing, with no error anywhere to say so.
    """
    documented = discovery.__doc__ or ""

    assert f'[project.entry-points."{ENTRY_POINT_GROUP}"]' in documented
    assert "transport_factory_locator" not in documented


def test_the_packaged_entry_points_use_the_group_the_code_reads() -> None:
    """pyproject.toml and the constant must agree, or nothing resolves."""
    manifest = (_REPO_ROOT / "pyproject.toml").read_text()

    assert f'[project.entry-points."{ENTRY_POINT_GROUP}"]' in manifest


def test_the_readme_documents_the_group_the_code_reads() -> None:
    """The README is where an adapter author copies the group name from."""
    readme = (_REPO_ROOT / "README.md").read_text()

    assert f'[project.entry-points."{ENTRY_POINT_GROUP}"]' in readme
