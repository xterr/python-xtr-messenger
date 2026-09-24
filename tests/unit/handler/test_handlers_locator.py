from __future__ import annotations

from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, final

from xtr_messenger import HandlersLocator

if TYPE_CHECKING:
    from xtr_messenger import HandlerDescriptor


class Base:
    pass


class Child(Base):
    pass


@final
@dataclass(frozen=True)
class Append:
    """A decorator with value equality, so an equal one is recognised as the same."""

    suffix: str

    def __call__(self, descriptor: HandlerDescriptor) -> HandlerDescriptor:
        return replace(descriptor, name=descriptor.name + self.suffix)


def test_handlers_of_every_ancestor_accumulate_most_specific_first() -> None:
    """Lookup once stopped at the nearest ancestor, silently switching off a base handler."""
    registry = HandlersLocator()

    async def on_base(message: Base) -> None:
        del message

    async def on_child(message: Child) -> None:
        del message

    _ = registry.register(Base, on_base, name="base")
    _ = registry.register(Child, on_child, name="child")

    assert [d.name for d in registry.handlers_for(Child)] == ["child", "base"]


def test_a_handler_registered_across_the_hierarchy_runs_once() -> None:
    registry = HandlersLocator()

    async def audit(message: Base) -> None:
        del message

    _ = registry.register(Base, audit, name="base")
    _ = registry.register(Child, audit, name="child")

    assert len(registry.handlers_for(Child)) == 1


def test_message_types_lists_every_type_with_a_handler() -> None:
    registry = HandlersLocator()

    async def handle(message: Base) -> None:
        del message

    _ = registry.register(Base, handle)

    assert registry.message_types() == (Base,)


def test_two_handlers_can_share_one_message_type() -> None:
    registry = HandlersLocator()

    async def first(message: Base) -> None:
        del message

    async def second(message: Base) -> None:
        del message

    _ = registry.register(Base, first)
    _ = registry.register(Base, second)

    assert len(registry.handlers_for(Base)) == 2


def test_decorate_applies_to_handlers_already_registered() -> None:
    registry = HandlersLocator()

    async def handle(message: Base) -> None:
        del message

    _ = registry.register(Base, handle)
    registry.decorate(Append("-wrapped"))

    assert registry.handlers_for(Base)[0].name == handle.__qualname__ + "-wrapped"


def test_decorate_applies_to_handlers_registered_later() -> None:
    registry = HandlersLocator()
    registry.decorate(Append("-wrapped"))

    async def handle(message: Base) -> None:
        del message

    _ = registry.register(Base, handle)

    assert registry.handlers_for(Base)[0].name == handle.__qualname__ + "-wrapped"


def test_decorators_apply_in_order_always_from_the_declared_descriptor() -> None:
    registry = HandlersLocator()

    async def handle(message: Base) -> None:
        del message

    _ = registry.register(Base, handle)
    registry.decorate(Append("-a"))
    registry.decorate(Append("-b"))

    assert registry.handlers_for(Base)[0].name == handle.__qualname__ + "-a-b"


def test_a_decorator_equal_to_an_applied_one_replaces_it_instead_of_stacking() -> None:
    """Rebinding to a new container must not leave the old binding stacked underneath."""
    registry = HandlersLocator()

    async def handle(message: Base) -> None:
        del message

    _ = registry.register(Base, handle)
    registry.decorate(Append("-x"))
    registry.decorate(Append("-x"))

    assert registry.handlers_for(Base)[0].name == handle.__qualname__ + "-x"
