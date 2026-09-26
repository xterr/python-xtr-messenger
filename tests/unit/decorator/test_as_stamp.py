"""Declaring a stamp class that serializers restore."""

from __future__ import annotations

from dataclasses import dataclass
from typing import cast

import pytest

from xtr_messenger import MessageBusError, NonSendableStampInterface, StampInterface, as_stamp
from xtr_messenger.stamp_registry import stamp_type_for


def test_it_registers_the_class_and_returns_it_unchanged() -> None:
    @dataclass(frozen=True, slots=True)
    class TenantProbeStamp(StampInterface):
        tenant_id: str

    assert as_stamp(TenantProbeStamp) is TenantProbeStamp
    assert stamp_type_for("TenantProbeStamp") is TenantProbeStamp


def test_two_classes_under_one_name_are_refused() -> None:
    def make() -> type[StampInterface]:
        @dataclass(frozen=True, slots=True)
        class ClashingProbeStamp(StampInterface):
            value: int

        return ClashingProbeStamp

    _ = as_stamp(make())

    with pytest.raises(MessageBusError, match="already used"):
        _ = as_stamp(make())


def test_a_stamp_that_never_leaves_the_process_is_refused() -> None:
    @dataclass(frozen=True, slots=True)
    class LocalProbeStamp(NonSendableStampInterface):
        value: int

    with pytest.raises(MessageBusError, match="not sendable"):
        _ = as_stamp(LocalProbeStamp)


def test_a_class_that_is_not_a_stamp_is_refused() -> None:
    class NotAStamp:
        pass

    with pytest.raises(MessageBusError, match="not a StampInterface"):
        _ = as_stamp(cast("type[StampInterface]", NotAStamp))
