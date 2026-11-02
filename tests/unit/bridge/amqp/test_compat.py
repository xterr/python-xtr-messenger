"""Every name the RabbitMQ transport had under ``xtr_messenger.bridge.amqp`` still resolves.

The transport moved to xtr-amqp-messenger; the old import paths stay through
this major version and hand back the very objects the new package defines.
"""

from __future__ import annotations

import importlib

import pytest

_MODULES = (
    "amqp_broker",
    "amqp_options",
    "amqp_transport_factory",
    "connection_options",
    "dead_letter_middleware",
    "dead_lettering",
    "exchange_options",
    "queue_options",
    "reliability",
)


@pytest.mark.parametrize("module", ["", *(f".{name}" for name in _MODULES)])
def test_every_old_name_is_the_moved_object(module: str) -> None:
    old = importlib.import_module(f"xtr_messenger.bridge.amqp{module}")
    new = importlib.import_module(f"xtr_amqp_messenger.transport{module}")
    names: list[str] = list(getattr(new, "__all__", []))

    assert names
    assert list(getattr(old, "__all__", [])) == names
    assert all(getattr(old, name) is getattr(new, name) for name in names)


def test_the_core_error_is_the_one_the_transport_raises() -> None:
    from xtr_amqp_messenger import MixedDsnError  # noqa: PLC0415 — the import is the assertion

    from xtr_messenger import MixedDsnError as CoreMixedDsnError  # noqa: PLC0415

    assert MixedDsnError is CoreMixedDsnError
