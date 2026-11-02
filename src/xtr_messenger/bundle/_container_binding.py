"""Binding every declared handler through the kernel's container."""

from __future__ import annotations

from dataclasses import replace
from typing import final

from typing_extensions import override
from xtr_dependency_injection import bind_callable
from xtr_service_contracts import ContainerInterface

from xtr_messenger.handler.handler_descriptor import HandlerDescriptor

__all__ = ["ContainerBinding"]


@final
class ContainerBinding:
    """Rebinds every :class:`HandlerDescriptor.call` through a container.

    Two instances are equal so :meth:`HandlersLocator.decorate` replaces an
    earlier binding instead of stacking on it — a handler declared after the
    kernel booted is bound to the current container only.
    """

    __slots__ = ("_container",)

    def __init__(self, container: ContainerInterface) -> None:
        self._container = container

    def __call__(self, descriptor: HandlerDescriptor) -> HandlerDescriptor:
        target = descriptor.handler
        bound = bind_callable(self._container, target)
        return replace(descriptor, call=bound)

    @override
    def __eq__(self, other: object) -> bool:
        return isinstance(other, ContainerBinding)

    @override
    def __hash__(self) -> int:
        return hash(ContainerBinding)
