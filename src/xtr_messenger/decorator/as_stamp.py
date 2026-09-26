"""Declare a stamp class that serializers restore."""

from __future__ import annotations

from typing import TypeVar

from xtr_messenger.exception import MessageBusError
from xtr_messenger.stamp import NonSendableStampInterface, StampInterface
from xtr_messenger.stamp_registry import register_stamp

__all__ = ["as_stamp"]

StampT = TypeVar("StampT", bound=type[StampInterface])


def as_stamp(stamp_type: StampT, /) -> StampT:
    """Declare ``stamp_type`` as a stamp that survives a real transport.

    A serializer built without an explicit ``stamp_types`` restores it on
    decode, beside the stamps the library ships::

        @as_stamp
        @dataclass(frozen=True, slots=True)
        class TenantStamp(StampInterface):
            tenant_id: str

    A stamp travels under its class name, so two declared classes may not
    share one.

    Raises:
        MessageBusError: If the class is not a stamp, is one that never leaves
            the process, or shares its name with another declared stamp.
    """
    if not issubclass(stamp_type, StampInterface):
        raise MessageBusError(f"{stamp_type.__qualname__} is not a StampInterface")
    if issubclass(stamp_type, NonSendableStampInterface):
        reason = "it never leaves the process, so there is nothing to restore"
        raise MessageBusError(f"{stamp_type.__qualname__} is not sendable: {reason}")
    register_stamp(stamp_type)
    return stamp_type
