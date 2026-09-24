"""A transport was asked for a worker but can neither be consumed nor build one."""

from __future__ import annotations

from .message_bus_error import MessageBusError

__all__ = ["NotConsumableError"]


class NotConsumableError(MessageBusError):
    """A transport sends but cannot be consumed, and brings no worker either.

    Reaching this means an adapter implements only the sending half and does
    not implement
    :class:`~xtr_messenger.worker_providing_interface.WorkerProvidingInterface`,
    so there is nothing to run and no way to build it.
    """

    names: tuple[str, ...]
    scheme: str

    def __init__(self, names: tuple[str, ...], scheme: str) -> None:
        """Record which transports were asked for, and their scheme."""
        self.names = names
        self.scheme = scheme
        joined = ", ".join(names)
        remedy = "its factory should implement WorkerProvidingInterface"
        super().__init__(f"cannot consume {joined}: it has no receive half, and {remedy}")
