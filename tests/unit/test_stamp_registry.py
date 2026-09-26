"""The stamp classes the application has declared sendable."""

from __future__ import annotations

from dataclasses import dataclass

import pytest

from xtr_messenger import DelayStamp, MessageBusError, StampInterface
from xtr_messenger.stamp_registry import declared_stamps, register_stamp, stamp_type_for


@dataclass(frozen=True, slots=True)
class RegistryProbeStamp(StampInterface):
    marker: str


def test_a_library_stamp_is_found_by_its_class_name() -> None:
    assert stamp_type_for("DelayStamp") is DelayStamp


def test_a_registered_stamp_is_found_and_listed() -> None:
    register_stamp(RegistryProbeStamp)

    assert stamp_type_for("RegistryProbeStamp") is RegistryProbeStamp
    assert RegistryProbeStamp in declared_stamps()


def test_registering_the_same_class_twice_is_harmless() -> None:
    register_stamp(RegistryProbeStamp)
    register_stamp(RegistryProbeStamp)

    assert declared_stamps().count(RegistryProbeStamp) == 1


def test_an_unknown_name_resolves_to_nothing() -> None:
    assert stamp_type_for("NoSuchStamp") is None


def test_a_class_named_like_a_library_stamp_is_refused() -> None:
    @dataclass(frozen=True, slots=True)
    class DelayStamp(StampInterface):
        delay_ms: int

    with pytest.raises(MessageBusError, match="'DelayStamp' is already used"):
        register_stamp(DelayStamp)
